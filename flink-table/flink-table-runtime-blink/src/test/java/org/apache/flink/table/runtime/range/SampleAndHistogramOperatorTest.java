/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.range;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.Projection;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.table.runtime.join.String2HashJoinOperatorTest.newRow;
import static org.junit.Assert.assertEquals;

/**
 * UT for SampleAndHistogramOperator.
 */
public class SampleAndHistogramOperatorTest implements Serializable {

	@Test
	@SuppressWarnings("unchecked")
	public void test() throws Exception {
		List<IntermediateSampleData<BinaryRow>> data = new ArrayList<>();
		data.add(new IntermediateSampleData(0.01, newRow("aaa", "0")));
		data.add(new IntermediateSampleData(0.02, newRow("zzz", "0")));
		data.add(new IntermediateSampleData(0.99, newRow("ddd", "1")));
		data.add(new IntermediateSampleData(0.5, newRow("gfi", "2")));
		data.add(new IntermediateSampleData(0.2, newRow("a", "0")));
		data.add(new IntermediateSampleData(0.7, newRow("mkl", "3")));
		data.add(new IntermediateSampleData(0.85, newRow("sdf", "7")));
		data.add(new IntermediateSampleData(0.7, newRow("hkl", "4")));
		data.add(new IntermediateSampleData(0.2, newRow("a", "0")));
		data.add(new IntermediateSampleData(0.3, newRow("a", "0")));
		data.add(new IntermediateSampleData(0.9, newRow("jkl", "5")));
		data.add(new IntermediateSampleData(0.8, newRow("xyz", "6")));
		data.add(new IntermediateSampleData(0.78, newRow("njk", "10")));
		data.add(new IntermediateSampleData(0.86, newRow("oji", "9")));
		data.add(new IntermediateSampleData(0.752, newRow("efg", "8")));
		data.add(new IntermediateSampleData(0.3, newRow("a", "0")));

		//sort fields:
		// ddd, efg, gfi,hkl, jkl, mkl, njk, oji, sdf, xyz
		BaseRowTypeInfo typeInfo = new BaseRowTypeInfo(
				DataTypes.STRING().getLogicalType(), DataTypes.STRING().getLogicalType());
		int[] keys = new int[]{0};
		boolean[] orders = new boolean[]{true};
		GeneratedProjection genProjection = new GeneratedProjection("", "", new Object[0]) {
			@Override
			public Projection newInstance(ClassLoader classLoader) {
				return input -> {
					BinaryRow row = new BinaryRow(1);
					BinaryRowWriter writer = new BinaryRowWriter(row);
					if (input.isNullAt(0)) {
						writer.setNullAt(0);
					} else {
						writer.writeString(0, input.getString(0));
					}
					writer.complete();
					return row;
				};
			}
		};
		GeneratedRecordComparator genComp = new GeneratedRecordComparator("", "", new Object[0]) {
			@Override
			public RecordComparator newInstance(ClassLoader classLoader) {
				return (RecordComparator) (o1, o2) -> o1.getString(0).compareTo(o2.getString(0));
			}
		};
		SampleAndHistogramOperator sampleAndHistogramOperator = new SampleAndHistogramOperator(
				10,
				genProjection,
				genComp,
				new KeyExtractor(keys, orders, new LogicalType[]{typeInfo.getLogicalTypes()[0]}),
				4);

		TypeInformation<IntermediateSampleData> inTypeInfo =
				TypeExtractor.getForClass(IntermediateSampleData.class);
		TypeInformation<Object[][]> outTypeInfo = TypeExtractor.getForClass(Object[][].class);
		OneInputStreamTaskTestHarness<IntermediateSampleData, Object[][]> testHarness =
				new OneInputStreamTaskTestHarness<>(OneInputStreamTask::new, 2, 2, inTypeInfo, outTypeInfo);

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(sampleAndHistogramOperator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		for (IntermediateSampleData sampleData : data) {
			testHarness.processElement(new StreamRecord<>(sampleData, 0));
		}

		testHarness.waitForInputProcessing();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		LinkedBlockingQueue<Object> output = testHarness.getOutput();

		assertEquals(1, output.size());

		Object[][] range = (Object[][]) ((StreamRecord) output.iterator().next()).getValue();
		assertEquals(3, range.length);
		assertEquals("gfi", range[0][0].toString());
		assertEquals("mkl", range[1][0].toString());
		assertEquals("oji", range[2][0].toString());
	}
}
