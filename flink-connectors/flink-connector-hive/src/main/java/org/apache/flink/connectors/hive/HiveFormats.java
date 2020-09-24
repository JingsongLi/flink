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

package org.apache.flink.connectors.hive;

import org.apache.flink.orc.OrcColumnarRowFileInputFormat;
import org.apache.flink.orc.OrcColumnarRowFileInputFormat.PartitionValueConverter;
import org.apache.flink.orc.nohive.OrcNoHiveColumnarRowInputFormat;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;

/**
 */
public class HiveFormats {
	private HiveFormats() {}

	private static final Logger LOG = LoggerFactory.getLogger(HiveFormats.class);

	public static boolean useOrcVectorizedRead(StorageDescriptor sd, int[] selectedFields, LogicalType[] fieldTypes) {
		boolean isOrc = sd.getSerdeInfo().getSerializationLib().toLowerCase().contains("orc");
		if (!isOrc) {
			return false;
		}

		for (int i : selectedFields) {
			if (isVectorizationUnsupported(fieldTypes[i])) {
				LOG.info("Fallback to hadoop mapred reader, unsupported field type: " + fieldTypes[i]);
				return false;
			}
		}

		LOG.info("Use flink orc ColumnarRowData reader.");
		return true;
	}

	private static boolean isVectorizationUnsupported(LogicalType t) {
		switch (t.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case DECIMAL:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return false;
			case TIMESTAMP_WITH_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
			case ARRAY:
			case MULTISET:
			case MAP:
			case ROW:
			case DISTINCT_TYPE:
			case STRUCTURED_TYPE:
			case NULL:
			case RAW:
			case SYMBOL:
			default:
				return true;
		}
	}

	public static OrcColumnarRowFileInputFormat<?> createOrcColumnarRowInputFormat(
			String hiveVersion,
			HiveShim shim,
			JobConf jobConf,
			RowType tableType,
			List<String> partitionKeys,
			int[] selectedFields,
			StorageDescriptor sd) {
		Configuration conf = new Configuration(jobConf);
		sd.getSerdeInfo().getParameters().forEach(conf::set);

		return hiveVersion.startsWith("1.") ?
				OrcNoHiveColumnarRowInputFormat.forNoHivePartitionTable(
						conf,
						tableType,
						partitionKeys,
						partValueConverter(shim),
						selectedFields,
						new ArrayList<>(),
						DEFAULT_SIZE) :
				OrcColumnarRowFileInputFormat.forHivePartitionTable(
						OrcShim.createShim(hiveVersion),
						conf,
						tableType,
						partitionKeys,
						partValueConverter(shim),
						selectedFields,
						new ArrayList<>(),
						DEFAULT_SIZE);
	}

	private static PartitionValueConverter partValueConverter(HiveShim shim) {
		return (PartitionValueConverter) (valStr, type) -> convertStringToPartValue(shim, valStr, type);
	}

	private static Object convertStringToPartValue(HiveShim shim, String valStr, LogicalType type) {
		LogicalTypeRoot typeRoot = type.getTypeRoot();
		DataType dataType = TypeConversions.fromLogicalToDataType(type);
		//note: it's not a complete list ofr partition key types that Hive support, we may need add more later.
		switch (typeRoot) {
			case CHAR:
			case VARCHAR:
				return valStr;
			case BOOLEAN:
				return Boolean.parseBoolean(valStr);
			case TINYINT:
				return Integer.valueOf(valStr).byteValue();
			case SMALLINT:
				return Short.valueOf(valStr);
			case INTEGER:
				return Integer.valueOf(valStr);
			case BIGINT:
				return Long.valueOf(valStr);
			case FLOAT:
				return Float.valueOf(valStr);
			case DOUBLE:
				return Double.valueOf(valStr);
			case DATE:
				return HiveInspectors.toFlinkObject(
						HiveInspectors.getObjectInspector(dataType),
						shim.toHiveDate(Date.valueOf(valStr)),
						shim);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return HiveInspectors.toFlinkObject(
						HiveInspectors.getObjectInspector(dataType),
						shim.toHiveTimestamp(Timestamp.valueOf(valStr)),
						shim);
			default:
				break;
		}
		throw new FlinkHiveException(
				new IllegalArgumentException(String.format("Can not convert %s to type %s for partition value", valStr, type)));
	}
}
