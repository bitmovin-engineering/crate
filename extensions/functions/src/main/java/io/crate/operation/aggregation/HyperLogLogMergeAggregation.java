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
 * Same as {@link HyperLogLogMergeDistinctAggregation} but returns the merged state as base64 text instead of
 * the cardinality, so states can be merged incrementally.
 */
public class HyperLogLogMergeAggregation extends AggregationFunction<HllState, String> {

    static final String NAME = "hyperloglog_merge";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .build(),
            HyperLogLogMergeAggregation::new
        );
    }

    private final Signature signature;
    private final BoundSignature boundSignature;
    private final HyperLogLogMergeDistinctAggregation mergeDistinctAggregation;

    private HyperLogLogMergeAggregation(Signature signature, BoundSignature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
        this.mergeDistinctAggregation = new HyperLogLogMergeDistinctAggregation(signature, boundSignature);
    }

    @Nullable
    @Override
    public HllState newState(RamAccounting ramAccounting,
                             Version minNodeInCluster,
                             MemoryManager memoryManager) {
        return mergeDistinctAggregation.newState(ramAccounting, minNodeInCluster, memoryManager);
    }

    @Override
    public HllState iterate(RamAccounting ramAccounting,
                            MemoryManager memoryManager,
                            HllState state,
                            Input<?>... args) throws CircuitBreakingException {
        return mergeDistinctAggregation.iterate(ramAccounting, memoryManager, state, args);
    }

    @Override
    public HllState reduce(RamAccounting ramAccounting, HllState state1, HllState state2) {
        return mergeDistinctAggregation.reduce(ramAccounting, state1, state2);
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
