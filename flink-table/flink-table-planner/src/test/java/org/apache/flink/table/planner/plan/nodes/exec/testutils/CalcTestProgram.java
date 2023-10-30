/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc1;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecCalc}. */
public class CalcTestProgram {

    static final TableTestProgram SIMPLE_CALC =
            TableTestProgram.of("simple-calc", "Simple calc with sources and sinks")
                    .runSql("INSERT INTO sink_t SELECT a + 1, b FROM t")
                    .setupTableSource("t")
                    .withSchema("a BIGINT", "b DOUBLE")
                    .withValuesBeforeRestore(Row.of(420L, 42.0))
                    .withValuesAfterRestore(Row.of(421L, 42.1))
                    .complete()
                    .setupTableSink("sink_t")
                    .withSchema("a BIGINT", "b DOUBLE")
                    .withValuesBeforeRestore(Row.of(421L, 42.0))
                    .withValuesAfterRestore(Row.of(421L, 42.0), Row.of(422L, 42.1))
                    .complete()
                    .build();


    static final TableTestProgram CALC_WITH_FILTER_PUSHDOWN =
            TableTestProgram.of("simple-calc-with-filter", "Simple calc with filter")
                    .runSql("INSERT INTO sink_t SELECT a, b FROM t WHERE a > CAST(420 AS BIGINT)")
                    .setupTableSource("t")
                    .withSchema("a BIGINT", "b DOUBLE")
                    .withOption("filterable-fields", "a")
                    .withValuesBeforeRestore(Row.of(421L, 42.1))
                    .withValuesAfterRestore(Row.of(421L, 42.1))
                    .complete()
                    .setupTableSink("sink_t")
                    .withSchema("a BIGINT", "b DOUBLE")
                    .withOption("filterable-fields", "a")
                    .withValuesBeforeRestore(Row.of(421L, 42.1))
                    .withValuesAfterRestore(Row.of(421L, 42.1), Row.of(421L, 42.1))
                    .complete()
                    .build();

    static final TableTestProgram CALC_WITH_PROJECT_PUSHDOWN =
            TableTestProgram.of("calc-with-project-pushdown", "Calc with project pushdown")
                    .runSql("INSERT INTO sink_t SELECT a, b, CAST(a as VARCHAR) FROM source_t WHERE a > 1")
                    .setupTableSource("source_t")
                        .withSchema("a INT", "b BIGINT", "c VARCHAR")
                    .withValues()
                        .withValuesBeforeRestore(Row.of(5, 421L, "hello"))
                        .withValuesAfterRestore(Row.of(5, 421L, "5"))
                        .complete()
                    .setupTableSink("sink_t")
                        .withSchema("a INT", "b BIGINT", "a1 VARCHAR")
                        .withValuesBeforeRestore(Row.of(5, 421L, "5"))
                        .withValuesAfterRestore(Row.of(5, 421L, "5"), Row.of(5, 421L, "5"))
                        .complete()
                    .build();

    static final TableTestProgram CALC_WITH_UDF =
            TableTestProgram.of("calc-with-udf", "Calc with UDF")
                    .setupTemporaryCatalogFunction("udf1", JavaFunc0.class)
                    .runSql("INSERT INTO sink_t SELECT a, udf1(a) FROM source_t")
                    .setupTableSource("source_t")
                    .withSchema("a INT")
                    .withValuesBeforeRestore(Row.of(5))
                    .withValuesAfterRestore(Row.of(5, 6L))
                    .complete()
                    .setupTableSink("sink_t")
                    .withSchema("a INT", "a1 BIGINT")
                    .withValuesBeforeRestore(Row.of(5, 6L))
                    .withValuesAfterRestore(Row.of(5, 6L), Row.of(5, 6L))
                    .complete()
                    .build();

}
