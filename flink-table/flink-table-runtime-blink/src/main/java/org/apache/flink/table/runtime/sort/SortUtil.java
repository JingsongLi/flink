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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.typeutils.base.NormalizedKeyUtil;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.nio.ByteOrder;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

/**
 * Util for sort.
 */
public class SortUtil {

	private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
	private static final int LONG_BYTES = 8;

	public static void minNormalizedKey(MemorySegment target, int offset, int numBytes) {
		//write min value.
		for (int i = 0; i < numBytes; i++) {
			target.put(offset + i, (byte) 0);
		}
	}

	/**
	 * Max unsigned byte is -1.
	 */
	public static void maxNormalizedKey(MemorySegment target, int offset, int numBytes) {
		//write max value.
		for (int i = 0; i < numBytes; i++) {
			target.put(offset + i, (byte) -1);
		}
	}

	public static void putShortNormalizedKey(short value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putShortNormalizedKey(value, target, offset, numBytes);
	}

	public static void putByteNormalizedKey(byte value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putByteNormalizedKey(value, target, offset, numBytes);
	}

	public static void putBooleanNormalizedKey(boolean value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putBooleanNormalizedKey(value, target, offset, numBytes);
	}

	/**
	 * UTF-8 supports bytes comparison.
	 */
	public static void putStringNormalizedKey(
			BinaryString value, MemorySegment target, int offset, int numBytes) {
		final int limit = offset + numBytes;
		final int end = value.getSizeInBytes();
		for (int i = 0; i < end && offset < limit; i++) {
			target.put(offset++, value.byteAt(i));
		}

		for (int i = offset; i < limit; i++) {
			target.put(i, (byte) 0);
		}
	}

	/**
	 * Just support the compact precision decimal.
	 */
	public static void putDecimalNormalizedKey(
			Decimal record, MemorySegment target, int offset, int len) {
		assert record.getPrecision() <= Decimal.MAX_COMPACT_PRECISION;
		putLongNormalizedKey(record.toUnscaledLong(), target, offset, len);
	}

	public static void putIntNormalizedKey(int value, MemorySegment target, int offset, int numBytes) {
		NormalizedKeyUtil.putIntNormalizedKey(value, target, offset, numBytes);
	}

	public static void putLongNormalizedKey(long value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putLongNormalizedKey(value, target, offset, numBytes);
	}

	/**
	 * See http://stereopsis.com/radix.html for more details.
	 */
	public static void putFloatNormalizedKey(float value, MemorySegment target, int offset,
			int numBytes) {
		int iValue = Float.floatToIntBits(value);
		iValue ^= ((iValue >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
		NormalizedKeyUtil.putUnsignedIntegerNormalizedKey(iValue, target, offset, numBytes);
	}

	/**
	 * See http://stereopsis.com/radix.html for more details.
	 */
	public static void putDoubleNormalizedKey(double value, MemorySegment target, int offset,
			int numBytes) {
		long lValue = Double.doubleToLongBits(value);
		lValue ^= ((lValue >> (Long.SIZE - 1)) | Long.MIN_VALUE);
		NormalizedKeyUtil.putUnsignedLongNormalizedKey(lValue, target, offset, numBytes);
	}

	public static void putBinaryNormalizedKey(
			byte[] value, MemorySegment target, int offset, int numBytes) {
		final int limit = offset + numBytes;
		final int end = value.length;
		for (int i = 0; i < end && offset < limit; i++) {
			target.put(offset++, value[i]);
		}

		for (int i = offset; i < limit; i++) {
			target.put(i, (byte) 0);
		}
	}

	public static int compareBinary(byte[] a, byte[] b) {
		return compareBinary(a, 0, a.length, b, 0, b.length);
	}

	public static int compareBinary(
			byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2) {
		// Short circuit equal case
		if (buffer1 == buffer2 &&
				offset1 == offset2 &&
				length1 == length2) {
			return 0;
		}
		int minLength = Math.min(length1, length2);
		int minWords = minLength / LONG_BYTES;
		int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
		int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 64-bit.
         */
		for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
			long lw = UNSAFE.getLong(buffer1, offset1Adj + (long) i);
			long rw = UNSAFE.getLong(buffer2, offset2Adj + (long) i);
			long diff = lw ^ rw;

			if (diff != 0) {
				if (!LITTLE_ENDIAN) {
					return lessThanUnsigned(lw, rw) ? -1 : 1;
				}

				// Use binary search
				int n = 0;
				int y;
				int x = (int) diff;
				if (x == 0) {
					x = (int) (diff >>> 32);
					n = 32;
				}

				y = x << 16;
				if (y == 0) {
					n += 16;
				} else {
					x = y;
				}

				y = x << 8;
				if (y == 0) {
					n += 8;
				}
				return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
			}
		}

		// The epilogue to cover the last (minLength % 8) elements.
		for (int i = minWords * LONG_BYTES; i < minLength; i++) {
			int result = unsignedByteToInt(buffer1[offset1 + i]) -
					unsignedByteToInt(buffer2[offset2 + i]);
			if (result != 0) {
				return result;
			}
		}
		return length1 - length2;
	}

	private static int unsignedByteToInt(byte value) {
		return value & 0xff;
	}

	private static boolean lessThanUnsigned(long x1, long x2) {
		return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
	}

	public static int compareNullable(LogicalType type, Object first, Object second) {
		// both values are null -> equality
		if (first == null && second == null) {
			return 0;
		}
		// first value is null -> inequality
		// but order is considered
		else if (first == null) {
			return -1;
		}
		// second value is null -> inequality
		// but order is considered
		else if (second == null) {
			return 1;
		}
		return compare(type, first, second);
	}

	/**
	 * Compare two internal data format with type.
	 */
	public static int compare(LogicalType type, Object first, Object second) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BOOLEAN:
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
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
			case ANY:
				return ((Comparable) first).compareTo(second);
			case BINARY:
			case VARBINARY:
				return compareBinary((byte[]) first, (byte[]) second);
			case ARRAY:
				return compareArray((ArrayType) type, (BaseArray) first, (BaseArray) second);
			case ROW:
				return compareRow((RowType) type, (BaseRow) first, (BaseRow) second);
			default:
				throw new RuntimeException("Not support yet: " + type);
		}
	}

	public static int compareArray(ArrayType type, BaseArray leftArray, BaseArray rightArray) {
		int minLen = Math.min(leftArray.numElements(), rightArray.numElements());
		for (int i = 0; i < minLen; i++) {
			boolean isNullLeft = leftArray.isNullAt(i);
			boolean isNullRight = rightArray.isNullAt(i);
			if (isNullLeft && isNullRight) {
				// Do nothing.
			} else if (isNullLeft) {
				return -1;
			} else if (isNullRight) {
				return 1;
			} else {
				LogicalType elementType = type.getElementType();
				Object o1 = TypeGetterSetters.get(leftArray, i, elementType);
				Object o2 = TypeGetterSetters.get(rightArray, i, elementType);
				int comp = compare(elementType, o1, o2);
				if (comp != 0) {
					return comp;
				}
			}
		}
		if (leftArray.numElements() < rightArray.numElements()) {
			return -1;
		} else if (rightArray.numElements() > rightArray.numElements()) {
			return 1;
		} else {
			return 0;
		}
	}

	public static int compareRow(RowType type, BaseRow leftRow, BaseRow rightRow) {
		for (int i = 0; i < type.getFieldCount(); i++) {
			boolean isNullLeft = leftRow.isNullAt(i);
			boolean isNullRight = rightRow.isNullAt(i);
			if (isNullLeft && isNullRight) {
				// Do nothing.
			} else if (isNullLeft) {
				return -1;
			} else if (isNullRight) {
				return 1;
			} else {
				LogicalType elementType = type.getTypeAt(i);
				Object o1 = TypeGetterSetters.get(leftRow, i, elementType);
				Object o2 = TypeGetterSetters.get(rightRow, i, elementType);
				int comp = compare(elementType, o1, o2);
				if (comp != 0) {
					return comp;
				}
			}
		}
		return 0;
	}
}
