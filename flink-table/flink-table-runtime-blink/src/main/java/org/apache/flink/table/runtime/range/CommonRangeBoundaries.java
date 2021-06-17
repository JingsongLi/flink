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

package org.apache.flink.table.runtime.range;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.distributions.RangeBoundaries;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.runtime.sort.SortUtil.compareNullable;

/**
 * split the records into multiple ranges.
 */
@Internal
public class CommonRangeBoundaries implements RangeBoundaries<BaseRow> {
	private final KeyExtractor keyExtractor;
	private final Object[][] boundaries;
	private final Object[] keys;
	private final LogicalType[] keyTypes;
	private final boolean[] orders;

	public CommonRangeBoundaries(KeyExtractor keyExtractor, Object[][] boundaries) {
		this.keyExtractor = keyExtractor;
		this.keyTypes = keyExtractor.getKeyTypes();
		this.orders = keyExtractor.getOrders();
		this.keys = new Object[keyTypes.length];
		this.boundaries = boundaries;
	}

	@Override
	public int getRangeIndex(BaseRow record) {
		return binarySearch(record);
	}

	// Search the range index of input record.
	private int binarySearch(BaseRow record) {
		int low = 0;
		int high = this.boundaries.length - 1;
		keyExtractor.extractKeys(record, keys, 0);

		while (low <= high) {
			final int mid = (low + high) >>> 1;
			final int result = compareKeys(this.boundaries[mid]);

			if (result > 0) {
				low = mid + 1;
			} else if (result < 0) {
				high = mid - 1;
			} else {
				return mid;
			}
		}
		// key not found, but the low index is the target
		// bucket, since the boundaries are the upper bound
		return low;
	}

	private int compareKeys(Object[] boundary) {
		if (keyTypes.length != keys.length || keyTypes.length != boundary.length) {
			throw new RuntimeException("Can not compare keys with boundary due to mismatched length.");
		}

		for (int i = 0; i < keyTypes.length; i++) {
			int result = compareNullable(keyTypes[i], keys[i], boundary[i]);
			if (result != 0) {
				return orders[i] ? result : -result;
			}
		}

		return 0;
	}
}
