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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import java.math.BigDecimal;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecGroupWindowAggregate}. */
public class LookupJoinTestPrograms {

    static final SourceTestStep CUSTOMERS =
            SourceTestStep.newBuilder("customers_t")  // static table
                    .addOption("disable-lookup", "false")
                    //.addOption("bounded", "false")
                    .addSchema(
                            "id INT PRIMARY KEY NOT ENFORCED",
                            "name STRING",
                            "city STRING",
                            "state STRING",
                            "zipcode INT")
                    .producedBeforeRestore(
                            Row.of(1, "Bob", "Mountain View", "California", 94043),
                            Row.of(2, "Alice", "San Francisco", "California", 95016),
                            Row.of(3, "Claire", "Austin", "Texas", 73301),
                            Row.of(4, "Shannon", "Boise", "Idaho", 83701),
                            Row.of(5, "Jake", "New York City", "New York", 10001)
                    )
                    .producedAfterRestore(
                            Row.of(1, "Bob", "San Jose", "California", 94089),
                            Row.of(3, "Claire", "Dallas", "Texas", 75201),
                            Row.of(6, "Joana", "Atlanta", "Georgia", 30033)
                    )
                    .build();

    static final SourceTestStep ORDERS =
            SourceTestStep.newBuilder("orders_t")
                    .addSchema(
                            "order_id INT",
                            "customer_id INT",
                            "total DOUBLE",
                            "order_time STRING",
                            "proc_time AS PROCTIME()")
                    .producedBeforeRestore(
                            Row.of(1, 3, 44.44, "2020-10-10 00:00:01"),
                            Row.of(2, 5, 100.02, "2020-10-10 00:00:02"),
                            Row.of(4, 2, 92.61, "2020-10-10 00:00:04"),
                            Row.of(3, 1, 23.89, "2020-10-10 00:00:03"),
                            Row.of(6, 4, 7.65, "2020-10-10 00:00:06"),
                            Row.of(5, 2, 12.78, "2020-10-10 00:00:05")
                    )
                    .producedAfterRestore(
                            Row.of(7, 6, 17.58, "2020-10-10 00:00:07"), // new customer
                            Row.of(9, 3, 143.21, "2020-10-10 00:00:08"), // updated zip codes
                            Row.of(11, 1, 257.03, "2020-10-10 00:00:12") // updated zip codes
                    )
                    .build();

    static final TableTestProgram LOOKUP_JOIN =
            TableTestProgram.of(
                            "lookup-join",
                            "lookup join between orders and static customers table")
                    .setupTableSource(CUSTOMERS)
                    .setupTableSource(ORDERS)
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink_t")
                                    .addSchema(
                                            "order_id INT",
                                            "total DOUBLE",
                                            "name STRING",
                                            "city STRING",
                                            "state STRING",
                                            "zipcode INT")
                                    .consumedBeforeRestore(
                                            "+I[1, 44.44, Claire, Austin, Texas, 73301]",
                                            "+I[2, 100.02, Jake, New York City, New York, 10001]",
                                            "+I[4, 92.61, Alice, San Francisco, California, 95016]",
                                            "+I[3, 23.89, Bob, Mountain View, California, 94043]",
                                            "+I[6, 7.65, Shannon, Boise, Idaho, 83701]",
                                            "+I[5, 12.78, Alice, San Francisco, California, 95016]")
                                    .consumedAfterRestore(
                                            "+I[7, 17.58, Joana, Atlanta, Georgia, 30033]",
                                            "+I[9, 143.21, Claire, Dallas, Texas, 75201]",
                                            "+I[11, 257.03, Bob, San Jose, California, 94089]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink_t SELECT "
                                    + "o.order_id, "
                                    + "o.total, "
                                    + "c.name, "
                                    + "c.city, "
                                    + "c.state, "
                                    + "c.zipcode "
                                    + "FROM orders_t as o "
                                    + "JOIN customers_t FOR SYSTEM_TIME AS OF o.proc_time AS c "
                                    + "ON o.customer_id = c.id")
                    .build();

}
