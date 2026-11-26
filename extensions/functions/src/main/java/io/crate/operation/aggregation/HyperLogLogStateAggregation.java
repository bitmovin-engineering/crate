/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.aggregation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar.Feature;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.HyperLogLogDistinctAggregation.HllState;
import io.crate.operation.aggregation.HyperLogLogDistinctAggregation.HllStateType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.UUIDType;

/**
 * Same as {@link HyperLogLogDistinctAggregation} but returns the aggregated HyperLogLog++ state as base64 text
 * instead of the cardinality, so it can be stored by clients and merged later via {@link HyperLogLogMergeAggregation}
 * or {@link HyperLogLogMergeDistinctAggregation}.
 */
public class HyperLogLogStateAggregation extends AggregationFunction<HllState, String> {

    static final String NAME = "hyperloglog_state";

    /// register hyperloglog_state(type[, precision::int])
    private static <T> void register(Functions.Builder builder, DataType<T> type) {
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(type.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .build(),
            (signature, boundSignature) -> new HyperLogLogStateAggregation(signature, boundSignature, type)
        );
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(
                    type.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .build(),
            (signature, boundSignature) -> new HyperLogLogStateAggregation(signature, boundSignature, type)
        );
    }

    public static void register(Functions.Builder builder) {
        for (var supportedType : DataTypes.PRIMITIVE_TYPES) {
            register(builder, supportedType);
        }
        register(builder, UUIDType.INSTANCE);
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final HyperLogLogDistinctAggregation distinctAggregation;

    private HyperLogLogStateAggregation(Signature signature, BoundSignature boundSignature, DataType<?> dataType) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.distinctAggregation = new HyperLogLogDistinctAggregation(signature, boundSignature, dataType);
    }

    @Nullable
    @Override
    public HllState newState(RamAccounting ramAccounting,
                             Version minNodeInCluster,
                             MemoryManager memoryManager) {
        return distinctAggregation.newState(ramAccounting, minNodeInCluster, memoryManager);
    }

    @Override
    public HllState iterate(RamAccounting ramAccounting,
                            MemoryManager memoryManager,
                            HllState state,
                            Input<?>... args) throws CircuitBreakingException {
        return distinctAggregation.iterate(ramAccounting, memoryManager, state, args);
    }

    @Override
    public HllState reduce(RamAccounting ramAccounting, HllState state1, HllState state2) {
        return distinctAggregation.reduce(ramAccounting, state1, state2);
    }

    @Nullable
    @Override
    public String terminatePartial(RamAccounting ramAccounting, HllState state) {
        if (state.isInitialized() == false) {
            return null;
        }
        try {
            return HllStateCodec.encode(state);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Nullable
    @Override
    public DocValueAggregator<?> getDocValueAggregator(LuceneReferenceResolver referenceResolver,
                                                       List<Reference> aggregationReferences,
                                                       DocTableInfo table,
                                                       Version shardCreatedVersion,
                                                       List<Literal<?>> optionalParams) {
        return distinctAggregation.getDocValueAggregator(
            referenceResolver, aggregationReferences, table, shardCreatedVersion, optionalParams);
    }

    @Override
    public DataType<?> partialType() {
        return HllStateType.INSTANCE;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public BoundSignature boundSignature() {
        return boundSignature;
    }
}
