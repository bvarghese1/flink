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

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSortLimit;
import org.apache.flink.table.planner.utils.InternalConfigOptions;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecSortLimit}. */
public class SortTestPrograms {

    static final TableTestProgram SORT_LIMIT_ASC =
            TableTestProgram.of(
                            "sort-limit-asc",
                            "validates sort limit node by sorting integers in asc mode")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .producedBeforeRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .producedAfterRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b VARCHAR", "c BIGINT")
                                    // Sort maintains a max heap of min elements so the final state
                                    // after producing test data of the heap
                                    // is shown below with every insertion and deletion shown in the
                                    // consumedBeforeRestore
                                    //      [3, b, 7]
                                    // [2, a, 6]  [1, a, 5]
                                    .consumedBeforeRestore(
                                            "+I[2, a, 6]",
                                            "+I[4, b, 8]",
                                            "+I[6, c, 10]",
                                            "-D[6, c, 10]",
                                            "+I[1, a, 5]",
                                            "-D[4, b, 8]",
                                            "+I[3, b, 7]")
                                    // Since the same data is replayed after restore the heap state
                                    // is restored and updated.
                                    // The final state of the heap is shown below with every
                                    // insertion and deletion shown in the consumedAfterRestore
                                    //      [2, a, 6]
                                    // [1, a, 5]  [1, a, 5]
                                    .consumedAfterRestore(
                                            "-D[3, b, 7]",
                                            "+I[2, a, 6]",
                                            "-D[2, a, 6]",
                                            "+I[1, a, 5]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * from source_t ORDER BY a LIMIT 3")
                    .build();

    static final TableTestProgram SORT_LIMIT_DESC =
            TableTestProgram.of(
                            "sort-limit-desc",
                            "validates sort limit node by sorting integers in desc mode")
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .producedBeforeRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .producedAfterRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b VARCHAR", "c BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[2, a, 6]",
                                            "+I[4, b, 8]",
                                            "+I[6, c, 10]",
                                            "-D[2, a, 6]",
                                            "+I[3, b, 7]",
                                            "-D[3, b, 7]",
                                            "+I[5, c, 9]")
                                    .consumedAfterRestore("-D[4, b, 8]", "+I[6, c, 10]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * from source_t ORDER BY a DESC LIMIT 3")
                    .build();

    static final TableTestProgram SORT_ASC =
            TableTestProgram.of(
                            "sort-asc",
                            "validates sort node by sorting integers in asc mode")
                    .setupConfig(InternalConfigOptions.TABLE_EXEC_NON_TEMPORAL_SORT_ENABLED, true)
                    .setupTableSource(
                            SourceTestStep.newBuilder("source_t")
                                    .addSchema("a INT", "b VARCHAR", "c INT")
                                    .addOption("terminating", "true")
                                    .producedBeforeRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .producedAfterRestore(
                                            Row.of(2, "a", 6),
                                            Row.of(4, "b", 8),
                                            Row.of(6, "c", 10),
                                            Row.of(1, "a", 5),
                                            Row.of(3, "b", 7),
                                            Row.of(5, "c", 9))
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema("a INT", "b VARCHAR", "c BIGINT")
                                    .consumedBeforeRestore(
                                            "+I[1, a, 5]",
                                            "+I[2, a, 6]",
                                            "+I[3, b, 7]",
                                            "+I[4, b, 8]",
                                            "+I[5, c, 9]",
                                            "+I[6, c, 10]")
                    .consumedAfterRestore("-D[4, b, 8]", "+I[6, c, 10]")
                                    .build())
                    .runSql("INSERT INTO sink_t SELECT * from source_t ORDER BY a")
                    .build();
}
