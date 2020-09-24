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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.vector.OrcVectorizedBatchWrapper;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.orc.OrcSplitReaderUtil.convertToOrcTypeWithPart;
import static org.apache.flink.orc.OrcSplitReaderUtil.getNonPartNames;
import static org.apache.flink.orc.OrcSplitReaderUtil.getSelectedOrcFields;
import static org.apache.flink.orc.vector.AbstractOrcColumnVector.createFlinkVector;
import static org.apache.flink.orc.vector.AbstractOrcColumnVector.createFlinkVectorFromConstant;

/**
 * An ORC reader that produces a stream of {@link ColumnarRowData} records.
 */
public class OrcColumnarRowFileInputFormat<BATCH> extends AbstractOrcFileInputFormat<RowData, BATCH> {

	private static final long serialVersionUID = 1L;

	private final ColumnBatchGenerator<BATCH> batchGenerator;
	private final RowType projectedOutputType;

	public OrcColumnarRowFileInputFormat(
			final OrcShim<BATCH> shim,
			final ColumnBatchGenerator<BATCH> batchGenerator,
			final Configuration hadoopConfig,
			final RowType projectedOutputType,
			final TypeDescription orcSchema,
			final int[] selectedFields,
			final List<OrcSplitReader.Predicate> conjunctPredicates,
			final int batchSize) {
		super(shim, hadoopConfig, orcSchema, selectedFields, conjunctPredicates, batchSize);
		this.batchGenerator = batchGenerator;
		this.projectedOutputType = projectedOutputType;
	}

	@Override
	public OrcReaderBatch<RowData, BATCH> createReaderBatch(
			final Path filePath,
			final OrcVectorizedBatchWrapper<BATCH> orcBatch,
			final Pool.Recycler<OrcReaderBatch<RowData, BATCH>> recycler,
			final int batchSize) {

		final VectorizedColumnBatch flinkColumnBatch = batchGenerator.generate(filePath, orcBatch.getBatch());
		return new VectorizedColumnReaderBatch<>(orcBatch, flinkColumnBatch, recycler);
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return InternalTypeInfo.of(projectedOutputType);
	}

	// ------------------------------------------------------------------------

	/**
	 * One batch of ORC columnar vectors and Flink column vectors.
	 *
	 */
	private static final class VectorizedColumnReaderBatch<BATCH> extends OrcReaderBatch<RowData, BATCH> {

		private final VectorizedColumnBatch flinkColumnBatch;
		private final ColumnarRowIterator result;

		VectorizedColumnReaderBatch(
				final OrcVectorizedBatchWrapper<BATCH> orcBatch,
				final VectorizedColumnBatch flinkColumnBatch,
				final Pool.Recycler<OrcReaderBatch<RowData, BATCH>> recycler) {
			super(orcBatch, recycler);
			this.flinkColumnBatch = flinkColumnBatch;
			this.result = new ColumnarRowIterator(new ColumnarRowData(flinkColumnBatch), this::recycle);
		}

		@Override
		public RecordIterator<RowData> convertAndGetIterator(
				final OrcVectorizedBatchWrapper<BATCH> orcBatch,
				final long startingOffset) {
			// no copying from the ORC column vectors to the Flink columns vectors necessary,
			// because they point to the same data arrays internally design
			int batchSize = orcBatch.size();
			flinkColumnBatch.setNumRows(batchSize);
			result.set(batchSize, startingOffset, 0L);
			return result;
		}
	}

	/**
	 * Interface to gen {@link VectorizedColumnBatch}.
	 */
	@FunctionalInterface
	public interface ColumnBatchGenerator<BATCH> extends Serializable {
		VectorizedColumnBatch generate(Path filePath, BATCH rowBatch);
	}

	/**
	 * Interface to convert partition value.
	 */
	@FunctionalInterface
	public interface PartitionValueConverter extends Serializable {
		Object convert(String valStr, LogicalType type);
	}

	public static OrcColumnarRowFileInputFormat<VectorizedRowBatch> forHivePartitionTable(
			OrcShim<VectorizedRowBatch> shim,
			Configuration conf,
			RowType tableType,
			List<String> partitionKeys,
			PartitionValueConverter partValueConverter,
			int[] selectedFields,
			List<OrcSplitReader.Predicate> conjunctPredicates,
			int batchSize) {
		String[] tableFieldNames = tableType.getFieldNames().toArray(new String[0]);
		LogicalType[] tableFieldTypes = tableType.getChildren().toArray(new LogicalType[0]);
		Set<String> partKeySet = new HashSet<>(partitionKeys);
		List<String> orcFieldNames = getNonPartNames(tableFieldNames, partKeySet);
		int[] orcSelectedFields = getSelectedOrcFields(tableFieldNames, selectedFields, orcFieldNames);

		ColumnBatchGenerator<VectorizedRowBatch> batchGenerator = (Path filePath, VectorizedRowBatch rowBatch) -> {
			LinkedHashMap<String, String> partitionSpec = PartitionPathUtils.extractPartitionSpecFromPath(filePath);

			// create and initialize the row batch
			ColumnVector[] vectors = new ColumnVector[selectedFields.length];
			for (int i = 0; i < vectors.length; i++) {
				String name = tableFieldNames[selectedFields[i]];
				LogicalType type = tableFieldTypes[selectedFields[i]];
				vectors[i] = partKeySet.contains(name) ?
						createFlinkVectorFromConstant(
								type,
								partValueConverter.convert(partitionSpec.get(name), type),
								batchSize) :
						createFlinkVector(rowBatch.cols[orcFieldNames.indexOf(name)], type);
			}
			return new VectorizedColumnBatch(vectors);
		};

		return new OrcColumnarRowFileInputFormat<>(
				shim,
				batchGenerator,
				conf,
				new RowType(Arrays.stream(selectedFields).mapToObj(i ->
						tableType.getFields().get(i)).collect(Collectors.toList())),
				convertToOrcTypeWithPart(tableFieldNames, tableFieldTypes, partKeySet),
				orcSelectedFields,
				conjunctPredicates,
				batchSize);
	}
}
