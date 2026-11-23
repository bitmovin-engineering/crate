/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.aggregation.impl;

import static io.crate.execution.engine.aggregation.impl.TopKAggregation.DEFAULT_LIMIT;
import static io.crate.execution.engine.aggregation.impl.TopKAggregation.DEFAULT_MAX_CAPACITY;
import static io.crate.execution.engine.aggregation.impl.TopKAggregation.initState;
import static io.crate.execution.engine.aggregation.impl.TopKAggregation.supportedByLongSketch;
import static io.crate.execution.engine.aggregation.impl.TopKAggregation.topKLongState;
import static io.crate.execution.engine.aggregation.impl.TopKAggregation.topKState;
import static io.crate.execution.engine.aggregation.impl.TopKAggregation.validateParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.network.NetworkUtils;
import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.execution.engine.aggregation.impl.TopKAggregation.Empty;
import io.crate.execution.engine.aggregation.impl.TopKAggregation.State;
import io.crate.execution.engine.aggregation.impl.TopKAggregation.StateType;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Scalar.Feature;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IpType;
import io.crate.types.LongType;
import io.crate.types.StringType;
import io.crate.types.TypeSignature;

/**
 * Weighted variant of {@link TopKAggregation}:
 *
 * <pre>
 *   topk_bitmovin(column, weight, [k], [max_capacity])
 * </pre>
 *
 * Instead of counting occurrences, each row adds {@code weight} to the frequency of {@code column}.
 * The result has the same shape as {@code topk}; {@code estimate}, {@code lower_bound},
 * {@code upper_bound} and {@code maximum_error} are expressed in summed weight.
 *
 * Rows with a null value or null weight are skipped; a weight of 0 is a no-op;
 * negative weights are rejected.
 */
public class TopKWeightedAggregation extends AggregationFunction<State, Map<String, Object>> {

    public static final String NAME = "topk_bitmovin";

    static final Signature WEIGHT_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.V,
                DataTypes.LONG.getTypeSignature())
            .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
            .features(Feature.DETERMINISTIC)
            .typeVariableConstraints(TypeVariableConstraint.V)
            .build();

    static final Signature WEIGHT_LIMIT_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.V,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature())
            .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
            .features(Feature.DETERMINISTIC)
            .typeVariableConstraints(TypeVariableConstraint.V)
            .build();

    static final Signature WEIGHT_LIMIT_CAPACITY_SIGNATURE =
        Signature.builder(NAME, FunctionType.AGGREGATE)
            .argumentTypes(TypeSignature.V,
                DataTypes.LONG.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature(),
                DataTypes.INTEGER.getTypeSignature())
            .returnType(DataTypes.UNTYPED_OBJECT.getTypeSignature())
            .features(Feature.DETERMINISTIC)
            .typeVariableConstraints(TypeVariableConstraint.V)
            .build();

    public static void register(Functions.Builder builder) {
        builder.add(WEIGHT_SIGNATURE, TopKWeightedAggregation::new);
        builder.add(WEIGHT_LIMIT_SIGNATURE, TopKWeightedAggregation::new);
        builder.add(WEIGHT_LIMIT_CAPACITY_SIGNATURE, TopKWeightedAggregation::new);
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final DataType<?> argumentType;

    private TopKWeightedAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        DataType<?> type = boundSignature.argTypes().getFirst();
        // In PARTIAL_FINAL mode the function is looked up with the partial StateType as argument,
        // see TopKAggregation constructor.
        if (type instanceof StateType st) {
            this.argumentType = st.innerType();
        } else {
            this.argumentType = type;
        }
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }

    @Nullable
    @Override
    public State newState(RamAccounting ramAccounting,
                          Version minNodeInCluster,
                          MemoryManager memoryManager) {
        return State.EMPTY;
    }

    @Override
    public State iterate(RamAccounting ramAccounting,
                         MemoryManager memoryManager,
                         State state,
                         Input<?>... args) throws CircuitBreakingException {
        if (state instanceof Empty) {
            int limit = DEFAULT_LIMIT;
            int capacity = DEFAULT_MAX_CAPACITY;
            if (args.length >= 3) {
                limit = (Integer) args[2].value();
            }
            if (args.length == 4) {
                capacity = (Integer) args[3].value();
            }
            state = initState(ramAccounting, argumentType, limit, capacity);
        }
        Object value = args[0].value();
        Long weight = (Long) args[1].value();
        if (value != null && weight != null) {
            state.update(value, checkWeight(weight), argumentType);
        }
        return state;
    }

    private static long checkWeight(long weight) {
        if (weight < 0) {
            throw new IllegalArgumentException(
                "Weight parameter for " + NAME + " must not be negative. Got: " + weight);
        }
        return weight;
    }

    @Override
    public State reduce(RamAccounting ramAccounting, State state1, State state2) {
        return state1.merge(state2);
    }

    @Override
    public Map<String, Object> terminatePartial(RamAccounting ramAccounting, State state) {
        return state.result(argumentType);
    }

    @Override
    public DataType<?> partialType() {
        return new StateType(argumentType);
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       Version shardCreatedVersion,
                                                       List<Literal<?>> optionalParams) {
        // topk_bitmovin(ref, weightRef, [limit], [capacity])
        // -> aggregationReferences[ref, weightRef, null, null], optionalParams[null, null, limit, capacity]
        if (aggregationReferences.size() < 2) {
            return null;
        }
        Reference valueRef = getAggReference(aggregationReferences);
        Reference weightRef = aggregationReferences.get(1);
        if (valueRef == null
            || weightRef == null
            || !weightRef.hasDocValues()
            || weightRef.granularity() != RowGranularity.DOC
            || weightRef.valueType().id() != LongType.ID) {
            return null;
        }
        int limit = DEFAULT_LIMIT;
        int capacity = DEFAULT_MAX_CAPACITY;
        if (optionalParams.size() >= 3) {
            Literal<?> limitLiteral = optionalParams.get(2);
            if (limitLiteral != null) {
                limit = (int) limitLiteral.value();
            }
        }
        if (optionalParams.size() == 4) {
            Literal<?> capacityLiteral = optionalParams.get(3);
            if (capacityLiteral != null) {
                capacity = (int) capacityLiteral.value();
            }
        }
        validateParams(limit, capacity);

        DataType<?> type = valueRef.valueType();
        String valueColumn = valueRef.storageIdent();
        String weightColumn = weightRef.storageIdent();
        final int finalLimit = limit;
        final int finalCapacity = capacity;
        if (supportedByLongSketch(type)) {
            return new WeightedNumericDocValueAggregator(
                valueColumn,
                weightColumn,
                ramAccounting -> topKLongState(ramAccounting, finalLimit, finalCapacity),
                type
            );
        } else if (type.id() == StringType.ID) {
            return new WeightedBinaryDocValueAggregator(
                valueColumn,
                weightColumn,
                ramAccounting -> topKState(ramAccounting, finalLimit, finalCapacity),
                type,
                BytesRefConverter.STRING
            );
        } else if (type.id() == IpType.ID) {
            return new WeightedBinaryDocValueAggregator(
                valueColumn,
                weightColumn,
                ramAccounting -> topKState(ramAccounting, finalLimit, finalCapacity),
                type,
                BytesRefConverter.IP
            );
        }
        return null;
    }

    private interface StateFactory {
        State create(RamAccounting ramAccounting);
    }

    private enum BytesRefConverter {
        STRING {
            @Override
            Object convert(BytesRef ref) {
                return ref.utf8ToString();
            }
        },
        IP {
            @Override
            Object convert(BytesRef ref) {
                return NetworkUtils.formatIPBytes(ref);
            }
        };

        abstract Object convert(BytesRef ref);
    }

    /**
     * Base for doc value aggregators reading the value column plus a long weight column per doc.
     * Like the single column aggregators, multi-valued docs are skipped.
     */
    private abstract static class WeightedDocValueAggregator implements DocValueAggregator<State> {

        final String valueColumn;
        final String weightColumn;
        final StateFactory stateFactory;
        final DataType<?> type;

        SortedNumericDocValues weights;

        WeightedDocValueAggregator(String valueColumn,
                                   String weightColumn,
                                   StateFactory stateFactory,
                                   DataType<?> type) {
            this.valueColumn = valueColumn;
            this.weightColumn = weightColumn;
            this.stateFactory = stateFactory;
            this.type = type;
        }

        @Override
        public State initialState(RamAccounting ramAccounting, MemoryManager memoryManager, Version minNodeVersion) {
            return stateFactory.create(ramAccounting);
        }

        @Override
        public void loadDocValues(LeafReaderContext reader) throws IOException {
            weights = DocValues.getSortedNumeric(reader.reader(), weightColumn);
            loadValueDocValues(reader);
        }

        abstract void loadValueDocValues(LeafReaderContext reader) throws IOException;

        /**
         * @return the weight of the doc, or -1 if the doc has no single weight value.
         */
        long weight(int doc) throws IOException {
            if (weights.advanceExact(doc) && weights.docValueCount() == 1) {
                return checkWeight(weights.nextValue());
            }
            return -1;
        }

        @Nullable
        @Override
        public Object partialResult(RamAccounting ramAccounting, State state) {
            return state;
        }
    }

    private static final class WeightedNumericDocValueAggregator extends WeightedDocValueAggregator {

        private SortedNumericDocValues values;

        WeightedNumericDocValueAggregator(String valueColumn,
                                          String weightColumn,
                                          StateFactory stateFactory,
                                          DataType<?> type) {
            super(valueColumn, weightColumn, stateFactory, type);
        }

        @Override
        void loadValueDocValues(LeafReaderContext reader) throws IOException {
            values = DocValues.getSortedNumeric(reader.reader(), valueColumn);
        }

        @Override
        public State apply(RamAccounting ramAccounting, int doc, State state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                long weight = weight(doc);
                if (weight >= 0) {
                    state.update(values.nextValue(), weight, type);
                }
            }
            return state;
        }
    }

    private static final class WeightedBinaryDocValueAggregator extends WeightedDocValueAggregator {

        private final BytesRefConverter converter;
        private SortedSetDocValues values;

        WeightedBinaryDocValueAggregator(String valueColumn,
                                         String weightColumn,
                                         StateFactory stateFactory,
                                         DataType<?> type,
                                         BytesRefConverter converter) {
            super(valueColumn, weightColumn, stateFactory, type);
            this.converter = converter;
        }

        @Override
        void loadValueDocValues(LeafReaderContext reader) throws IOException {
            values = DocValues.getSortedSet(reader.reader(), valueColumn);
        }

        @Override
        public State apply(RamAccounting ramAccounting, int doc, State state) throws IOException {
            if (values.advanceExact(doc) && values.docValueCount() == 1) {
                long weight = weight(doc);
                if (weight >= 0) {
                    state.update(converter.convert(values.lookupOrd(values.nextOrd())), weight, type);
                }
            }
            return state;
        }
    }
}
