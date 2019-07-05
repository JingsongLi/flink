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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.sampling.IntermediateSampleData
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.io.network.DataExchangeMode
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, PartitionTransformation, ShuffleMode, TwoInputTransformation}
import org.apache.flink.streaming.runtime.partitioner.{BroadcastPartitioner, CustomPartitionerWrapper, ForwardPartitioner, GlobalPartitioner, RebalancePartitioner}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.sort.SortCodeGenerator
import org.apache.flink.table.codegen.{CodeGeneratorContext, HashCodeGenerator, ProjectionCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.common.CommonPhysicalExchange
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.BinaryHashPartitioner
import org.apache.flink.table.runtime.range.{AssignRangeIndexOperator, FirstIntFieldKeyExtractor, IdPartitioner, KeyExtractor, LocalSampleOperator, RemoveRangeIndexOperator, SampleAndHistogramOperator}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
  * This RelNode represents a change of partitioning of the input elements.
  *
  * This does not create a physical transformation If its relDistribution' type is not range,
  * it only affects how upstream operations are connected to downstream operations.
  *
  * But if the type is range, this relNode will create some physical transformation because it
  * need calculate the data distribution. To calculate the data distribution, the received stream
  * will split in two process stream. For the first process stream, it will go through the sample
  * and statistics to calculate the data distribution in pipeline mode. For the second process
  * stream will been bocked. After the first process stream has been calculated successfully,
  * then the two process stream  will union together. Thus it can partitioner the record based
  * the data distribution. Then The RelNode will create the following transformations.
  *
  * +---------------------------------------------------------------------------------------------+
  * |                                                                                             |
  * | +-----------------------------+                                                             |
  * | |       Transformation        | ------------------------------------>                       |
  * | +-----------------------------+                                     |                       |
  * |                 |                                                   |                       |
  * |                 |                                                   |                       |
  * |                 |forward & PIPELINED                                |                       |
  * |                \|/                                                  |                       |
  * | +--------------------------------------------+                      |                       |
  * | | OneInputTransformation[LocalSample, n]     |                      |                       |
  * | +--------------------------------------------+                      |                       |
  * |                      |                                              |forward & BATCH        |
  * |                      |forward & PIPELINED                           |                       |
  * |                     \|/                                             |                       |
  * | +--------------------------------------------------+                |                       |
  * | |OneInputTransformation[SampleAndHistogram, 1]     |                |                       |
  * | +--------------------------------------------------+                |                       |
  * |                        |                                            |                       |
  * |                        |broadcast & PIPELINED                       |                       |
  * |                        |                                            |                       |
  * |                       \|/                                          \|/                      |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               TwoInputTransformation[AssignRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                       |                                                     |
  * |                                       |custom & PIPELINED                                   |
  * |                                      \|/                                                    |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               OneInputTransformation[RemoveRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                                                                             |
  * +---------------------------------------------------------------------------------------------+
  */
class BatchExecExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, inputRel, relDistribution)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  private val SIP_NAME = "RangePartition: LocalSample"
  private val SIC_NAME = "RangePartition: SampleAndHistogram"
  private val ARI_NAME = "RangePartition: PreparePartition"
  private val PR_NAME = "RangePartition: Partition"
  private val TOTAL_SAMPLE_SIZE = 655360
  private val TOTAL_RANGES_NUM = 65536

  // TODO reuse PartitionTransformation
  // currently, an Exchange' input transformation will be reused if it is reusable,
  // and different PartitionTransformation objects will be created which have same input.
  // cache input transformation to reuse
  private var reusedInput: Option[Transformation[BaseRow]] = None
  // cache sampleAndHistogram transformation to reuse when distribution is RANGE
  private var reusedSampleAndHistogram: Option[Transformation[Array[Array[AnyRef]]]] = None
  // the required exchange mode for reusable ExchangeBatchExec
  // if it's None, use value from getDataExchangeMode
  private var requiredExchangeMode: Option[DataExchangeMode] = None

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): BatchExecExchange = {
    new BatchExecExchange(cluster, traitSet, newInput, relDistribution)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("exchange_mode", requiredExchangeMode.orNull,
        requiredExchangeMode.contains(DataExchangeMode.BATCH))
  }

  //~ ExecNode methods -----------------------------------------------------------

  def setRequiredDataExchangeMode(exchangeMode: DataExchangeMode): Unit = {
    require(exchangeMode != null)
    requiredExchangeMode = Some(exchangeMode)
  }

  private[flink] def getDataExchangeMode(tableConf: Configuration): DataExchangeMode = {
    requiredExchangeMode match {
      case Some(mode) if mode eq DataExchangeMode.BATCH => mode
      case _ => DataExchangeMode.PIPELINED
    }
  }

  override def getDamBehavior: DamBehavior = {
    distribution.getType match {
      case RelDistribution.Type.RANGE_DISTRIBUTED => DamBehavior.FULL_DAM
      case _ => DamBehavior.PIPELINED
    }
  }

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    getInputs.map(_.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): Transformation[BaseRow] = {
    val input = reusedInput match {
      case Some(transformation) => transformation
      case None =>
        val input = getInputNodes.get(0).translateToPlan(tableEnv)
            .asInstanceOf[Transformation[BaseRow]]
        reusedInput = Some(input)
        input
    }

    val inputType = input.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outputRowType = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val shuffleMode = requiredExchangeMode match {
      case None => ShuffleMode.PIPELINED
      case Some(mode) =>
        mode match {
          case DataExchangeMode.BATCH => ShuffleMode.BATCH
          case DataExchangeMode.PIPELINED => ShuffleMode.PIPELINED
        }
    }

    relDistribution.getType match {
      case RelDistribution.Type.ANY =>
        val transformation = new PartitionTransformation(
          input,
          null,
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.SINGLETON =>
        val transformation = new PartitionTransformation(
          input,
          new GlobalPartitioner[BaseRow],
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.RANGE_DISTRIBUTED =>
        getRangePartitionPlan(inputType, tableEnv, input)

      case RelDistribution.Type.RANDOM_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new RebalancePartitioner[BaseRow],
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.BROADCAST_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new BroadcastPartitioner[BaseRow],
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.HASH_DISTRIBUTED =>
        // TODO Eliminate duplicate keys
        val keys = relDistribution.getKeys
        val partitioner = new BinaryHashPartitioner(
          HashCodeGenerator.generateRowHash(
            CodeGeneratorContext(tableEnv.config),
            RowType.of(inputType.getLogicalTypes: _*),
            "HashPartitioner",
            keys.map(_.intValue()).toArray),
          keys.map(getInput.getRowType.getFieldNames.get(_)).toArray
        )
        val transformation = new PartitionTransformation(
          input,
          partitioner,
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation
      case _ =>
        throw new UnsupportedOperationException(
          s"not support RelDistribution: ${relDistribution.getType} now!")
    }
  }

  /**
    * The RelNode with range-partition distribution will create the following transformations.
    *
    * ------------------------- BATCH --------------------------> [B, m] -->...
    * /                                                            /
    * [A, n] --> [LocalSample, n] --> [SampleAndHistogram, 1] --BROADCAST-<
    * \                                                            \
    * ------------------------- BATCH --------------------------> [C, o] -->...
    *
    * Transformations of LocalSample and SampleAndHistogram can be reused.
    * The streams except the sample and histogram process stream will been blocked,
    * so the the sample and histogram process stream does not care about requiredExchangeMode.
    */
  protected def getRangePartitionPlan(
      inputType: BaseRowTypeInfo,
      tableEnv: BatchTableEnvironment,
      input: Transformation[BaseRow]): Transformation[BaseRow] = {
    val fieldCollations = relDistribution.asInstanceOf[FlinkRelDistribution].getFieldCollations.get
    val conf = tableEnv.getConfig

    val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations)
    val types = inputType.getLogicalTypes

    val sampleAndHistogram = reusedSampleAndHistogram match {
      case Some(transformation) => transformation
      case None =>
        // 1. Fixed size sample in each partitions.
        val localSampleOutRowType = RowType.of(keys.map(types(_)): _ *)

        val localSampleProjection = ProjectionCodeGenerator.generateProjection(
          CodeGeneratorContext(tableEnv.getConfig),
          "LocalSample",
          inputType.toRowType,
          localSampleOutRowType,
          keys,
          reusedOutRecord = false)

        val isdType = TypeExtractor.getForClass(classOf[IntermediateSampleData[BaseRow]])
        val localSample = new OneInputTransformation(
          input,
          SIP_NAME,
          new LocalSampleOperator(localSampleProjection, TOTAL_SAMPLE_SIZE),
          isdType,
          input.getParallelism)

        // 2. Fixed size sample in a single coordinator
        // and use sampled data to build range boundaries.
        val sampleTypes = keys.map(types(_))
        val sampleType = RowType.of(sampleTypes: _*)
        val ctx = CodeGeneratorContext(tableEnv.getConfig)
        val copyToBinaryRow = ProjectionCodeGenerator.generateProjection(
          ctx,
          "CopyToBinaryRow",
          localSampleOutRowType,
          sampleType,
          localSampleOutRowType.getChildren.indices.toArray,
          reusedOutRecord = false)
        val boundariesType = TypeExtractor.getForClass(classOf[Array[Array[AnyRef]]])
        val newKeyIndexes = keys.indices.toArray
        val generator = new SortCodeGenerator(
          conf,
          newKeyIndexes,
          sampleTypes,
          orders,
          nullsIsLast)
        val sampleAndHistogram = new OneInputTransformation(
          localSample,
          SIC_NAME,
          new SampleAndHistogramOperator(
            TOTAL_SAMPLE_SIZE,
            copyToBinaryRow,
            generator.generateRecordComparator("SampleAndHistogramComparator"),
            new KeyExtractor(
              newKeyIndexes,
              orders,
              newKeyIndexes.map(sampleTypes(_))),
            TOTAL_RANGES_NUM),
          boundariesType,
          1)
        reusedSampleAndHistogram = Some(sampleAndHistogram)
        sampleAndHistogram
    }

    // 3. Add broadcast shuffle
    val broadcast: PartitionTransformation[Array[Array[AnyRef]]] = new PartitionTransformation(
      sampleAndHistogram,
      new BroadcastPartitioner[Array[Array[AnyRef]]],
      ShuffleMode.PIPELINED)

    // 4. add batch dataExchange
    val batchExchange: PartitionTransformation[BaseRow] = new PartitionTransformation(
      input,
      new ForwardPartitioner[BaseRow],
      ShuffleMode.BATCH)

    // 4. Take range boundaries as broadcast input and take the tuple of partition id and
    // record as output.
    // TwoInputTransformation, it must be binaryRow.
    val binaryType = new BaseRowTypeInfo(types: _*)
    val preparePartition =
      new TwoInputTransformation[Array[Array[AnyRef]], BaseRow, Tuple2[Integer, BaseRow]](
        broadcast,
        batchExchange,
        ARI_NAME,
        new AssignRangeIndexOperator(
          new KeyExtractor(keys, orders, keys.map(binaryType.getLogicalTypes()(_)))),
        new TupleTypeInfo[Tuple2[Integer, BaseRow]](BasicTypeInfo.INT_TYPE_INFO, binaryType),
        input.getParallelism)

    // 5. Add shuffle according range partition.
    val rangePartition = new PartitionTransformation(
      preparePartition,
      new CustomPartitionerWrapper(
        new IdPartitioner(TOTAL_RANGES_NUM),
        new FirstIntFieldKeyExtractor),
      ShuffleMode.PIPELINED)

    // 6. Remove the partition id.
    new OneInputTransformation(
      rangePartition,
      PR_NAME,
      new RemoveRangeIndexOperator(),
      binaryType,
      getResource.getParallelism)
  }
}

