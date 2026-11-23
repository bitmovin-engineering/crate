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

import static io.crate.testing.TestingHelpers.printedTable;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class HyperLogLogStateMergeIntegrationTest extends IntegTestCase {

    private static final int NUM_DAYS = 5;

    @Test
    public void test_states_of_daily_queries_merge_to_same_result_as_single_query() {
        execute("create table sessions (day int, country text, user_id text) with (number_of_replicas = 0)");
        List<Integer> days = new ArrayList<>();
        List<String> countries = new ArrayList<>();
        List<String> userIds = new ArrayList<>();
        // user ids overlap between days so that the merge has to deduplicate across chunks
        for (int day = 1; day <= NUM_DAYS; day++) {
            for (int i = 0; i < 300; i++) {
                days.add(day);
                countries.add(i % 3 == 0 ? "AT" : "DE");
                userIds.add("user-" + (day * 100 + i));
            }
        }
        execute(
            "insert into sessions (day, country, user_id) (select * from unnest(?, ?, ?))",
            new Object[]{days, countries, userIds});
        execute("refresh table sessions");

        execute("select country, hyperloglog_distinct(user_id, 12) from sessions group by country order by country");
        String expected = printedTable(response.rows());

        List<String> stateCountries = new ArrayList<>();
        List<String> states = new ArrayList<>();
        for (int day = 1; day <= NUM_DAYS; day++) {
            execute(
                "select country, hyperloglog_state(user_id, 12) from sessions where day = ? group by country",
                new Object[]{day});
            for (Object[] row : response.rows()) {
                stateCountries.add((String) row[0]);
                states.add((String) row[1]);
            }
        }
        assertThat(states).hasSize(NUM_DAYS * 2);

        execute(
            "select country, hyperloglog_merge(state) from unnest(?, ?) as t (country, state) " +
            "group by country order by country",
            new Object[]{stateCountries, states});
        assertThat(printedTable(response.rows())).isEqualTo(expected);
    }
}
