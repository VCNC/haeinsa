/**
 * Copyright (C) 2013 VCNC, inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Sets;

/**
 * Tracking parameters of {@link HaeinsaScan} and {@link HaeinsaIntraScan}
 * inside {@link HaeinsaTable}.
 * <p>
 * Tracker can determine whether specific HaeinsaKeyValue inside scan range.
 */
public class HaeinsaColumnTracker {
	// { family -> qualifier }
	private final Map<byte[], NavigableSet<byte[]>> familyMap =
			new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);

	private final byte[] minColumn;
	private final boolean minColumnInclusive;
	private final byte[] maxColumn;
	private final boolean maxColumnInclusive;

	/**
	 * Constructor of HaeinsaColumnTracker.
	 * <p>
	 * If this ColumnTracker track {@link HaeinsaScan}, minColumn, maxColumn
	 * should be null and minColumnInclusive, maxColumnInclusive should be
	 * false.
	 *
	 * @param familyMap
	 *
	 * @param minColumn
	 * @param minColumnInclusive
	 * @param maxColumn
	 * @param maxColumnInclusive
	 */
	public HaeinsaColumnTracker(Map<byte[], NavigableSet<byte[]>> familyMap,
			byte[] minColumn, boolean minColumnInclusive,
			byte[] maxColumn, boolean maxColumnInclusive) {
		this.minColumn = minColumn;
		this.maxColumn = maxColumn;
		this.minColumnInclusive = minColumnInclusive;
		this.maxColumnInclusive = maxColumnInclusive;
		for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
			if (entry.getValue() != null) {
				NavigableSet<byte[]> qualifierSet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
				qualifierSet.addAll(entry.getValue());
				this.familyMap.put(entry.getKey(), qualifierSet);
			} else {
				this.familyMap.put(entry.getKey(), null);
			}
		}
	}

	/**
	 * Return true if qualifier of kv is placed between minColumn and maxColumn.
	 * <p>
	 * Using lexicographical ordering to compare byte[]
	 *
	 * @param kv
	 * @return
	 */
	public boolean isColumnInclusive(HaeinsaKeyValue kv) {
		int cmpMin = 1;
		if (this.minColumn != null) {
			cmpMin = Bytes.compareTo(kv.getQualifier(), minColumn);
		}

		if (cmpMin < 0) {
			return false;
		}

		if (!this.minColumnInclusive && cmpMin == 0) {
			return false;
		}

		// If there is no maxColumn requirement and satisfy minColumn's.
		if (this.maxColumn == null) {
			return true;
		}

		int cmpMax = Bytes.compareTo(kv.getQualifier(), maxColumn);

		if (this.maxColumnInclusive && cmpMax <= 0
				|| !this.maxColumnInclusive && cmpMax < 0) {
			return true;
		}

		return false;
	}

	/**
	 * Check whether kv is inside range of scan. There are three possibilities.
	 * <ol>
	 * <li>If scan do not specify any column family -
	 * return true if kv is inside qualifier range.</li>
	 * <li>If scan specify column families, but not any qualifiers. -
	 * return true when kv has same column family and is inside scan range.</li>
	 * <li>If scan specify both column families and column qualifiers -
	 * return true only if kv satisfy both requirement.</li>
	 * </ol>
	 *
	 * @param kv HaeinsaKeyValue which will be checked.
	 * @return
	 */
	public boolean isMatched(HaeinsaKeyValue kv) {
		// If familyMap is empty, then Haeinsa transaction assumes
		// that programmer wants to scan all (family, qualifier) pairs
		// inside scan range, and call isColumnInclusive(kv) directly.
		// { empty }
		if (familyMap.isEmpty()) {
			return isColumnInclusive(kv);
		}

		NavigableSet<byte[]> set = familyMap.get(kv.getFamily());
		// If column family is specified, but there are no qualifiers.
		// { family -> null }
		if (familyMap.containsKey(kv.getFamily()) && set == null) {
			return isColumnInclusive(kv);
		}

		// If both family and qualifier are specified.
		// { family -> qualifier }
		if (set.contains(kv.getQualifier())) {
			return isColumnInclusive(kv);
		}
		return false;
	}
}
