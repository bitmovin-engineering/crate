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

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.Version;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.SearchPath;
import io.crate.operation.aggregation.HyperLogLogDistinctAggregation.HllState;
import io.crate.types.DataTypes;

public class HyperLogLogStateMergeAggregationTest extends AggregationTestCase {

    private static final int NUM_VALUES = 10_000;
    private static final int NUM_CHUNKS = 30;
    private static final int PRECISION = 12;

    @Before
    public void prepareFunctions() {
        nodeCtx = createNodeContext(null, null);
    }

    @SuppressWarnings("unchecked")
    private <TFinal> AggregationFunction<HllState, TFinal> getFunction(String name, List<Symbol> args) {
        return (AggregationFunction<HllState, TFinal>) nodeCtx.functions().get(
            null, name, args, SearchPath.pathWithPGCatalogAndDoc());
    }

    private <TFinal> TFinal aggregate(AggregationFunction<HllState, TFinal> function, List<Object[]> rows) {
        HllState state = function.newState(RamAccounting.NO_ACCOUNTING, Version.CURRENT, memoryManager);
        for (Object[] row : rows) {
            Literal<?>[] inputs = new Literal<?>[row.length];
            for (int i = 0; i < row.length; i++) {
                inputs[i] = Literal.ofUnchecked(DataTypes.guessType(row[i]), row[i]);
            }
            state = function.iterate(RamAccounting.NO_ACCOUNTING, memoryManager, state, inputs);
        }
        return function.terminatePartial(RamAccounting.NO_ACCOUNTING, state);
    }

    private String aggregateStateOfLongs(List<Object[]> rows) {
        return aggregate(getFunction(HyperLogLogStateAggregation.NAME, List.of(Literal.of(1L), Literal.of(PRECISION))), rows);
    }

    @Test
    public void test_merged_chunk_states_equal_distinct_count_over_all_values() {
        List<Object[]> allRows = new ArrayList<>();
        for (long i = 0; i < NUM_VALUES; i++) {
            allRows.add(new Object[]{i, PRECISION});
        }
        Long expected = aggregate(
            getFunction(HyperLogLogDistinctAggregation.NAME, List.of(Literal.of(1L), Literal.of(PRECISION))), allRows);

        List<Object[]> encodedChunkStates = new ArrayList<>();
        int chunkSize = NUM_VALUES / NUM_CHUNKS + 1;
        for (int from = 0; from < NUM_VALUES; from += chunkSize) {
            List<Object[]> chunk = allRows.subList(from, Math.min(from + chunkSize, NUM_VALUES));
            encodedChunkStates.add(new Object[]{aggregateStateOfLongs(chunk)});
        }
        assertThat(encodedChunkStates).hasSize(NUM_CHUNKS);

        AggregationFunction<HllState, Long> merge = getFunction(HyperLogLogMergeAggregation.NAME, List.of(Literal.of("x")));
        assertThat(aggregate(merge, encodedChunkStates)).isEqualTo(expected);

        // incremental: fold the chunk states one by one into a running state
        AggregationFunction<HllState, String> mergeState =
            getFunction(HyperLogLogMergeStateAggregation.NAME, List.of(Literal.of("x")));
        String runningState = null;
        for (Object[] chunkState : encodedChunkStates) {
            runningState = aggregate(mergeState, List.<Object[]>of(new Object[]{runningState}, chunkState));
        }
        assertThat(aggregate(merge, List.<Object[]>of(new Object[]{runningState}))).isEqualTo(expected);
    }

    @Test
    public void test_state_without_rows_is_null_and_merge_without_states_is_0() {
        assertThat(aggregateStateOfLongs(List.<Object[]>of())).isNull();
        AggregationFunction<HllState, Long> merge = getFunction(HyperLogLogMergeAggregation.NAME, List.of(Literal.of("x")));
        assertThat(aggregate(merge, List.<Object[]>of())).isEqualTo(0L);
        assertThat(aggregate(merge, List.<Object[]>of(new Object[]{null}))).isEqualTo(0L);
    }

    @Test
    public void test_merge_rejects_states_with_different_precision() {
        String stateWithPrecision12 = aggregateStateOfLongs(List.<Object[]>of(new Object[]{1L, 12}));
        String stateWithPrecision14 = aggregateStateOfLongs(List.<Object[]>of(new Object[]{1L, 14}));
        AggregationFunction<HllState, Long> merge = getFunction(HyperLogLogMergeAggregation.NAME, List.of(Literal.of("x")));
        assertThatThrownBy(() -> aggregate(merge, List.<Object[]>of(new Object[]{stateWithPrecision12}, new Object[]{stateWithPrecision14})))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("cannot merge hyperloglog states with different precisions: 12 and 14");
    }

    @Test
    public void test_merge_rejects_invalid_state() {
        AggregationFunction<HllState, Long> merge = getFunction(HyperLogLogMergeAggregation.NAME, List.of(Literal.of("x")));
        assertThatThrownBy(() -> aggregate(merge, List.<Object[]>of(new Object[]{"not a state"})))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("hyperloglog state is not valid base64");
    }
}
