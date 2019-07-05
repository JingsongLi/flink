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

package org.apache.flink.table.runtime.utils

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.io.{OutputFormat, RichOutputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.sinks.{OutputFormatTableSink, TableSink}
import org.apache.flink.types.Row

import scala.collection.mutable

/**
  * A simple [[TableSink]] to collect rows of every partition.
  *
  */
@Internal
class RowsCollectTableSink extends OutputFormatTableSink[Row] {

  private var rowsCollectOutputFormat: RowsCollectOutputFormat = _
  private var fieldNames: Array[String] = _
  private var fieldTypes: Array[TypeInformation[_]] = _

  override def getOutputFormat: OutputFormat[Row] = rowsCollectOutputFormat

  override def getOutputType: RowTypeInfo = {
    new RowTypeInfo(getFieldTypes: _*)
  }

  def init(typeSerializer: TypeSerializer[Seq[Row]], id: String): Unit = {
    this.rowsCollectOutputFormat = new RowsCollectOutputFormat(typeSerializer, id)
  }

  override def configure(
      fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): RowsCollectTableSink = {
    this.fieldNames = fieldNames
    this.fieldTypes = fieldTypes
    this
  }

  override def getFieldNames = fieldNames

  override def getFieldTypes = fieldTypes
}

class RowsCollectOutputFormat(typeSerializer: TypeSerializer[Seq[Row]], id: String)
    extends RichOutputFormat[Row] {

  private var accumulator: SerializedListAccumulator[Seq[Row]] = _
  private var rows: mutable.ArrayBuffer[Row] = _

  override def writeRecord(record: Row): Unit = {
    this.rows += Row.copy(record)
  }

  override def configure(parameters: Configuration): Unit = {
  }

  override def close(): Unit = {
    val rowSeq: Seq[Row] = rows
    accumulator.add(rowSeq, typeSerializer)
    getRuntimeContext.addAccumulator(id, accumulator)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    this.accumulator = new SerializedListAccumulator[Seq[Row]]
    this.rows = new mutable.ArrayBuffer[Row]
  }
}
