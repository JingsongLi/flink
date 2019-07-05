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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.utils.SortTestUtils.{getOrderedRows, sortExpectedly, tupleDataSetStrings}
import org.apache.flink.table.runtime.utils.TestData.{data3, nullablesOfData3, type3}
import org.apache.flink.table.runtime.utils.{BatchTestBase, RowsCollectTableSink}
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.apache.flink.util.AbstractID

import org.junit._
import org.junit.rules.ExpectedException

import scala.collection.JavaConverters._

class SortITCase extends BatchTestBase {

  private val expectedException = ExpectedException.none()

  @Before
  def setUp(): Unit = {
    conf.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, true)
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
  }

  @Test
  def testOrderByMultipleFields(): Unit = {
    val sqlQuery = "SELECT * FROM Table3 ORDER BY a DESC, b DESC"

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) => (
        - x.productElement(0).asInstanceOf[Int],
        - x.productElement(1).asInstanceOf[Long]))

    val expected = sortExpectedly(tupleDataSetStrings)
    val tableSink = new RowsCollectTableSink
    val typeInformation = TypeExtractor.getForClass(classOf[Seq[Row]])
    val typeSerializer = typeInformation.createSerializer(env.getConfig)
    val id = new AbstractID().toString
    tableSink.init(typeSerializer, id)
    val table = tEnv.sqlQuery(sqlQuery)
    tEnv.writeToSink(table, tableSink.configure(Array("a", "b", "c"), type3.getFieldTypes))

    val result = getOrderedRows(tEnv, typeSerializer, id)
    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }
}
