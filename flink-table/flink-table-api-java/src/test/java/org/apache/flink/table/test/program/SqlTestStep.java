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

package org.apache.flink.table.test.program;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Test step for executing SQL.
 *
 * <p>Note: Not every runner supports generic SQL statements. Sometimes the runner would like to
 * enrich properties e.g. of a CREATE TABLE. Use this step with caution.
 */
public final class SqlTestStep implements TestStep {

    public final String sql;

    SqlTestStep(String sql) {
        this.sql = sql;
    }

    @Override
    public TestKind getKind() {
        return TestKind.SQL;
    }

    public TableResult apply(TableEnvironment env) {
        return env.executeSql(sql);
    }
}
