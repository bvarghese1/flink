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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Test}

import java.sql.Timestamp
import java.time.{Instant, ZoneId}

import scala.collection.mutable

class TimestampITCase extends StreamingTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()

    val ints = List(1, 2, 3, 4, null)

    val longs = List(1L, 2L, 2L, 4L, null)

    val datetimes = List(
      localDateTime("1969-01-01 00:00:00.123456789"),
      localDateTime("1970-01-01 00:00:00.123456"),
      localDateTime("1970-01-01 00:00:00.123456"),
      localDateTime("1970-01-01 00:00:00.123"),
      null
    )

    val timestamps = List(
      Timestamp.valueOf("1969-01-01 00:00:00.123456789").toLocalDateTime,
      Timestamp.valueOf("1970-01-01 00:00:00.123456").toLocalDateTime,
      Timestamp.valueOf("1970-01-01 00:00:00.123").toLocalDateTime,
      Timestamp.valueOf("1972-01-01 00:00:00").toLocalDateTime,
      Timestamp.valueOf("1973-01-01 00:00:00").toLocalDateTime
    )

    val instants = new mutable.MutableList[Instant]
    for (i <- datetimes.indices) {
      if (datetimes(i) == null) {
        instants += null
      } else {
        // Assume the time zone of source side is UTC
        instants +=
          datetimes(i).toInstant(ZoneId.of("UTC").getRules.getOffset(datetimes(i)))
      }
    }

    val data = new mutable.MutableList[Row]

    for (i <- ints.indices) {
      data += row(ints(i), longs(i), datetimes(i), timestamps(i), instants(i))
    }

    val dataId = TestValuesTableFactory.registerData(data.seq)
    // TODO: support high precision TIMESTAMP as timeAttributes
    //  LegacyTypeInfoDataTypeConverter does not support TIMESTAMP(p) where p > 3
    //  see TableSourceValidation::validateTimestampExtractorArguments
    tEnv.executeSql(s"""
                       |create table T (
                       |  a int,
                       |  b bigint,
                       |  c timestamp(9),
                       |  d timestamp(3),
                       |  e timestamp_ltz(9),
                       |  watermark for d as d
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'true',
                       |  'data-id' = '$dataId'
                       |)
                       |""".stripMargin)
  }

  @Test
  def testGroupByTimestamp(): Unit = {
    val sink = new TestingRetractSink()
    tEnv
      .sqlQuery("SELECT COUNT(a), c FROM T GROUP BY c")
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()
    val expected = Seq(
      "0,null",
      "1,1969-01-01T00:00:00.123456789",
      "1,1970-01-01T00:00:00.123",
      "2,1970-01-01T00:00:00.123456"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testGroupByLocalZonedTimestamp(): Unit = {
    val sink = new TestingRetractSink()
    tEnv
      .sqlQuery("SELECT COUNT(a), e FROM T GROUP BY e")
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()
    val expected = Seq(
      "0,null",
      "1,1969-01-01T00:00:00.123456789Z",
      "1,1970-01-01T00:00:00.123Z",
      "2,1970-01-01T00:00:00.123456Z"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testCountDistinctOnTimestamp(): Unit = {
    val sink = new TestingRetractSink()
    tEnv
      .sqlQuery("SELECT COUNT(DISTINCT c), b FROM T GROUP BY b")
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()
    val expected = Seq(
      "0,null",
      "1,1",
      "1,2",
      "1,4"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testCountDistinctOnLocalZonedTimestamp(): Unit = {
    val sink = new TestingRetractSink()
    tEnv
      .sqlQuery("SELECT COUNT(DISTINCT e), b FROM T GROUP BY b")
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()
    val expected = Seq(
      "0,null",
      "1,1",
      "1,2",
      "1,4"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testMaxMinOnTimestamp(): Unit = {
    val sink = new TestingRetractSink()
    tEnv
      .sqlQuery("SELECT MAX(c), MIN(c), b FROM T GROUP BY b")
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()
    val expected = Seq(
      "1969-01-01T00:00:00.123456789,1969-01-01T00:00:00.123456789,1",
      "null,null,null",
      "1970-01-01T00:00:00.123456,1970-01-01T00:00:00.123456,2",
      "1970-01-01T00:00:00.123,1970-01-01T00:00:00.123,4"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testMaxMinWithRetractOnTimestamp(): Unit = {
    val sink = new TestingRetractSink()
    tEnv
      .sqlQuery(s"""
                   |SELECT MAX(y), MIN(x)
                   |FROM
                   |  (SELECT b, MAX(c) AS x, MIN(c) AS y FROM T GROUP BY b, c)
                   |GROUP BY b
       """.stripMargin)
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()
    val expected = Seq(
      "1969-01-01T00:00:00.123456789,1969-01-01T00:00:00.123456789",
      "1970-01-01T00:00:00.123,1970-01-01T00:00:00.123",
      "1970-01-01T00:00:00.123456,1970-01-01T00:00:00.123456",
      "null,null"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }
}
