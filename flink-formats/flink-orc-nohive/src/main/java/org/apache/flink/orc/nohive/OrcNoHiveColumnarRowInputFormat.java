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

package org.apache.flink.orc.nohive;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcColumnarRowFileInputFormat;
import org.apache.flink.orc.OrcColumnarRowFileInputFormat.ColumnBatchGenerator;
import org.apache.flink.orc.OrcColumnarRowFileInputFormat.PartitionValueConverter;
import org.apache.flink.orc.OrcSplitReader;
import org.apache.flink.orc.nohive.shim.OrcNoHiveShim;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.orc.OrcSplitReaderUtil.convertToOrcTypeWithPart;
import static org.apache.flink.orc.OrcSplitReaderUtil.getNonPartNames;
import static org.apache.flink.orc.OrcSplitReaderUtil.getSelectedOrcFields;
import static org.apache.flink.orc.nohive.vector.AbstractOrcNoHiveVector.createFlinkVector;
import static org.apache.flink.orc.vector.AbstractOrcColumnVector.createFlinkVectorFromConstant;

/**
 * Helper class to create {@link OrcColumnarRowFileInputFormat} for no-hive.
 */
public class OrcNoHiveColumnarRowInputFormat {
	private OrcNoHiveColumnarRowInputFormat() {}

	public static OrcColumnarRowFileInputFormat<VectorizedRowBatch> forNoHivePartitionTable(
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
				vectors[i] = partitionSpec.containsKey(name) ?
						createFlinkVectorFromConstant(
								type,
								partValueConverter.convert(partitionSpec.get(name), type),
								batchSize) :
						createFlinkVector(rowBatch.cols[orcFieldNames.indexOf(name)]);
			}
			return new VectorizedColumnBatch(vectors);
		};
		return new OrcColumnarRowFileInputFormat<>(
				new OrcNoHiveShim(),
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
