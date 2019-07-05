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

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;

import static org.apache.flink.table.dataformat.TypeGetterSetters.get;

/**
 * for extract keys.
 */
public class KeyExtractor implements Serializable {

	private final int[] keyPositions;
	private final boolean[] orders;
	private final LogicalType[] keyTypes;

	public KeyExtractor(int[] keyPositions, boolean[] orders, LogicalType[] keyTypes) {
		this.keyPositions = keyPositions;
		this.orders = orders;
		this.keyTypes = keyTypes;
	}

	public int getKeyCount() {
		return keyTypes.length;
	}

	public LogicalType[] getKeyTypes() {
		return keyTypes;
	}

	public boolean[] getOrders() {
		return orders;
	}

	public void extractKeys(BaseRow record, Object[] target, int index) {
		int len = keyTypes.length;
		for (int i = 0; i < len; i++) {
			int pos = keyPositions[i];
			Object element = record.isNullAt(pos) ? null : get(record, pos, keyTypes[pos]);
			target[index + i] = element;
		}
	}
}
