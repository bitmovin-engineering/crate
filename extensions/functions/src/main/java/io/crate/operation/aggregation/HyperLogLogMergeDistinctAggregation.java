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

import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.aggregation.impl.HyperLogLogPlusPlus;
import io.crate.memory.MemoryManager;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar.Feature;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.operation.aggregation.HyperLogLogDistinctAggregation.HllState;
import io.crate.operation.aggregation.HyperLogLogDistinctAggregation.HllStateType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * Merges base64 encoded states produced by {@link HyperLogLogStateAggregation} and returns the cardinality
 * of the union. All states must have been created with the same precision.
 */
public class HyperLogLogMergeDistinctAggregation extends AggregationFunction<HllState, Long> {

    static final String NAME = "hyperloglog_merge_distinct";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.LONG.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .build(),
            HyperLogLogMergeDistinctAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;

    HyperLogLogMergeDistinctAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Nullable
    @Override
    public HllState newState(RamAccounting ramAccounting,
                             Version minNodeInCluster,
                             MemoryManager memoryManager) {
        return new HllState();
    }

    @Override
    public HllState iterate(RamAccounting ramAccounting,
                            MemoryManager memoryManager,
                            HllState state,
                            Input<?>... args) throws CircuitBreakingException {
        String encodedState = DataTypes.STRING.sanitizeValue(args[0].value());
        if (encodedState == null) {
            return state;
        }
        HllState decodedState;
        try {
            decodedState = HllStateCodec.decode(encodedState);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // only the first decoded state is retained as the group's state, every later one is merged into it
        if (state.isInitialized() == false) {
            ramAccounting.addBytes(HyperLogLogPlusPlus.memoryUsage(HllStateCodec.precision(decodedState)));
        }
        return reduce(ramAccounting, state, decodedState);
    }

    @Override
    public HllState reduce(RamAccounting ramAccounting, HllState state1, HllState state2) {
        if (state1.isInitialized() == false) {
            return state2;
        }
        if (state2.isInitialized()) {
            int precision1 = HllStateCodec.precision(state1);
            int precision2 = HllStateCodec.precision(state2);
            if (precision1 != precision2) {
                throw new IllegalArgumentException(
                    "cannot merge hyperloglog states with different precisions: " + precision1 + " and " + precision2);
            }
            state1.merge(state2);
        }
        return state1;
    }

    @Override
    public Long terminatePartial(RamAccounting ramAccounting, HllState state) {
        return state.isInitialized() ? state.value() : 0L;
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
