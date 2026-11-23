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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.operation.aggregation.AggregationTestCase;
import io.crate.testing.DataTypeTesting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class TopKWeightedAggregationTest extends AggregationTestCase {

    @Test
    public void test_weighted_top_k_longs() throws Exception {
        execute_weighted_top_k_without_and_with_doc_values(DataTypes.LONG);
    }

    @Test
    public void test_weighted_top_k_doubles() throws Exception {
        execute_weighted_top_k_without_and_with_doc_values(DataTypes.DOUBLE);
    }

    @Test
    public void test_weighted_top_k_integer() throws Exception {
        execute_weighted_top_k_without_and_with_doc_values(DataTypes.INTEGER);
    }

    @Test
    public void test_weighted_top_k_strings() throws Exception {
        execute_weighted_top_k_without_and_with_doc_values(DataTypes.STRING);
    }

    @Test
    public void test_weighted_top_k_timestamps() throws Exception {
        execute_weighted_top_k_without_and_with_doc_values(DataTypes.TIMESTAMPZ);
    }

    @Test
    public void test_weighted_top_k_ip() throws Exception {
        execute_weighted_top_k_without_and_with_doc_values(DataTypes.IP);
    }

    @Test
    public void test_weighted_top_k_ranks_by_weight_not_by_count() throws Exception {
        // "a" occurs most often, "b" has the highest summed weight
        Object[][] data = {
            new Object[]{"a", 1L},
            new Object[]{"a", 1L},
            new Object[]{"a", 1L},
            new Object[]{"b", 100L},
            new Object[]{"c", null},
            new Object[]{null, 50L},
            new Object[]{"d", 0L},
        };
        var result = executeAggregation(
            TopKWeightedAggregation.WEIGHT_SIGNATURE,
            List.of(DataTypes.STRING, DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );
        assertThat(result).isEqualTo(
            Map.of("maximum_error", 0L,
                "frequencies", List.of(
                    Map.of("item", "b", "estimate", 100L, "lower_bound", 100L, "upper_bound", 100L),
                    Map.of("item", "a", "estimate", 3L, "lower_bound", 3L, "upper_bound", 3L)
                )
            )
        );
    }

    @Test
    public void test_weighted_top_k_negative_weight() throws Exception {
        Object[][] data = {
            new Object[]{"a", -1L},
        };
        assertThatThrownBy(() -> executeAggregation(
            TopKWeightedAggregation.WEIGHT_SIGNATURE,
            List.of(DataTypes.STRING, DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
            false,
            List.of()
        )).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Weight parameter for topk_bitmovin must not be negative. Got: -1");
    }

    @Test
    public void test_weighted_top_k_invalid_capacity_parameters() throws Exception {
        Object[][] data = {
            new Object[]{"a", 1L, 1, 5},
        };
        assertThatThrownBy(() -> executeAggregation(
            TopKWeightedAggregation.WEIGHT_LIMIT_CAPACITY_SIGNATURE,
            List.of(DataTypes.STRING, DataTypes.LONG, DataTypes.INTEGER, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            data,
            false,
            List.of(Literal.of(1), Literal.of(5))
        )).isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Capacity parameter must be a positive integer-power of 2. Got: 5");
    }

    private void execute_weighted_top_k_without_and_with_doc_values(DataType<?> dataType) throws Exception {
        var generator = DataTypeTesting.getDataGenerator(dataType);
        var first = generator.get();
        var second = generator.get();
        while (second.equals(first)) {
            second = generator.get();
        }
        var third = generator.get();
        while (third.equals(second) || (third.equals(first))) {
            third = generator.get();
        }

        // first: count 4, weight 4; second: count 1, weight 30; third: count 2, weight 20
        Object[][] data = {
            new Object[]{first, 1L},
            new Object[]{first, 1L},
            new Object[]{first, 1L},
            new Object[]{first, 1L},
            new Object[]{second, 30L},
            new Object[]{null, 99L},
            new Object[]{third, null},
            new Object[]{third, 10L},
            new Object[]{third, 10L},
        };

        assertHasDocValueAggregator(TopKWeightedAggregation.NAME, List.of(dataType, DataTypes.LONG));

        var result = executeAggregation(
            TopKWeightedAggregation.WEIGHT_SIGNATURE,
            List.of(dataType, DataTypes.LONG),
            DataTypes.UNTYPED_OBJECT,
            data,
            true,
            List.of()
        );

        assertThat(result).isEqualTo(
            Map.of("maximum_error", 0L,
                "frequencies", List.of(
                    Map.of("item", second, "estimate", 30L, "lower_bound", 30L, "upper_bound", 30L),
                    Map.of("item", third, "estimate", 20L, "lower_bound", 20L, "upper_bound", 20L),
                    Map.of("item", first, "estimate", 4L, "lower_bound", 4L, "upper_bound", 4L)
                )
            )
        );

        int limit = 2;
        Object[][] dataWithLimit = {
            new Object[]{first, 1L, limit},
            new Object[]{first, 1L, limit},
            new Object[]{second, 30L, limit},
            new Object[]{third, 10L, limit},
            new Object[]{third, 10L, limit},
        };

        result = executeAggregation(
            TopKWeightedAggregation.WEIGHT_LIMIT_SIGNATURE,
            List.of(dataType, DataTypes.LONG, DataTypes.INTEGER),
            DataTypes.UNTYPED_OBJECT,
            dataWithLimit,
            true,
            List.of(Literal.of(limit))
        );

        assertThat(result).isEqualTo(
            Map.of("maximum_error", 0L,
                "frequencies", List.of(
                    Map.of("item", second, "estimate", 30L, "lower_bound", 30L, "upper_bound", 30L),
                    Map.of("item", third, "estimate", 20L, "lower_bound", 20L, "upper_bound", 20L)
                )
            )
        );
    }
}
