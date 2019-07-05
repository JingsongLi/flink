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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Global sample and histogram for range partition.
 */
@Internal
public class SampleAndHistogramOperator extends TableStreamOperator<Object[][]>
		implements OneInputStreamOperator<IntermediateSampleData<BaseRow>, Object[][]>, BoundedOneInput {

	private final ReservoirSamplerWithoutReplacement sampler;
	private GeneratedRecordComparator genComparator;
	private GeneratedProjection genProjection;
	private final int rangesNum;
	private final KeyExtractor keyExtractor;

	private transient Collector<Object[][]> collector;
	private transient RecordComparator comparator;

	public SampleAndHistogramOperator(
			int numSample, GeneratedProjection genProjection, GeneratedRecordComparator genComparator,
			KeyExtractor keyExtractor, int rangesNum) {
		sampler = new ReservoirSamplerWithoutReplacement(numSample, 0L);
		this.genComparator = genComparator;
		this.rangesNum = rangesNum;
		this.genProjection = genProjection;
		this.keyExtractor = keyExtractor;
	}

	@Override
	public void open() throws Exception {
		super.open();
		sampler.setProjection(genProjection.newInstance(getUserCodeClassloader()));
		this.collector = new StreamRecordCollector<>(output);
		this.comparator = genComparator.newInstance(getUserCodeClassloader());
		genProjection = null;
		genComparator = null;
	}

	@Override
	public void processElement(StreamRecord<IntermediateSampleData<BaseRow>> streamRecord)
			throws Exception {
		sampler.collectSampleData(streamRecord.getValue());
	}

	@Override
	public void endInput() throws Exception {
		Iterator<IntermediateSampleData<BaseRow>> sampled = sampler.sample();

		List<BaseRow> sampledData = new ArrayList<>();
		while (sampled.hasNext()) {
			sampledData.add(sampled.next().getElement());
		}
		sampledData.sort(comparator);

		int boundarySize = rangesNum - 1;
		Object[][] boundaries = new Object[boundarySize][];
		if (sampledData.size() > 0) {
			double avgRange = sampledData.size() / (double) rangesNum;
			int numKey = keyExtractor.getKeyCount();
			for (int i = 1; i < rangesNum; i++) {
				BaseRow record = sampledData.get((int) (i * avgRange));
				Object[] keys = new Object[numKey];
				keyExtractor.extractKeys(record, keys, 0);
				boundaries[i - 1] = keys;
			}
		}

		collector.collect(boundaries);
	}

}
