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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.generated.Projection;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.table.runtime.join.String2HashJoinOperatorTest.newRow;
import static org.junit.Assert.assertEquals;

/**
 * UT for LocalSampleOperator.
 */
public class LocalSampleOperatorTest implements Serializable {

	@Test
	@SuppressWarnings("unchecked")
	public void testLocalSample() throws Exception {
		List<BinaryRow> data = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			data.add(newRow(String.valueOf(i), String.valueOf(i + 1)));
		}
		BaseRowTypeInfo inTypeInfo = new BaseRowTypeInfo(
				DataTypes.STRING().getLogicalType(), DataTypes.STRING().getLogicalType());
		GeneratedProjection generatedProjection = new GeneratedProjection("", "", new Object[0]) {
			@Override
			public Projection newInstance(ClassLoader classLoader) {
				return input -> {
					BinaryRow row = new BinaryRow(2);
					BinaryRowWriter writer = new BinaryRowWriter(row);
					if (input.isNullAt(1)) {
						writer.setNullAt(0);
					} else {
						writer.writeString(0, input.getString(1));
					}
					if (input.isNullAt(0)) {
						writer.setNullAt(1);
					} else {
						writer.writeString(1, input.getString(0));
					}
					writer.complete();
					return row;
				};
			}
		};

		LocalSampleOperator operator = new LocalSampleOperator(generatedProjection, 3);

		TypeInformation<IntermediateSampleData> outTypeInfo =
				TypeExtractor.getForClass(IntermediateSampleData.class);
		OneInputStreamTaskTestHarness<BinaryRow, IntermediateSampleData> testHarness =
				new OneInputStreamTaskTestHarness<>(OneInputStreamTask::new, 2, 2, (TypeInformation) inTypeInfo, outTypeInfo);

		testHarness.setupOutputForSingletonOperatorChain();
		testHarness.getStreamConfig().setStreamOperator(operator);
		testHarness.getStreamConfig().setOperatorID(new OperatorID());

		long initialTime = 0L;

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		for (BinaryRow row : data) {
			testHarness.processElement(new StreamRecord<>(row, initialTime));
		}

		testHarness.waitForInputProcessing();
		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		LinkedBlockingQueue<Object> queue = testHarness.getOutput();
		assertEquals(3, queue.size());
		for (Object object : queue) {
			IntermediateSampleData sampleData = ((StreamRecord<IntermediateSampleData>) object).getValue();
			BinaryRow binaryRow = (BinaryRow) (sampleData.getElement());
			assertEquals(2, binaryRow.getArity());
			System.out.println("weight: " + sampleData.getWeight() +
					" data " + " "  + binaryRow.getString(0) + " " + binaryRow.getString(1));
		}
	}
}
