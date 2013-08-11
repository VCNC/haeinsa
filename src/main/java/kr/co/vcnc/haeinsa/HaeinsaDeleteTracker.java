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

import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Tracking deleted columns and family inside specific row.
 */
public class HaeinsaDeleteTracker {
	// { family -> sequenceId }
	private final NavigableMap<byte[], Long> families = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	// { family -> { column -> sequenceId } }
	private final NavigableMap<byte[], NavigableMap<byte[], Long>> cells = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

	/**
	 * Update family map or column map if kv is not exist in map or sequenceId
	 * is lower.
	 *
	 * @param kv - HaeinsaKeyValue to track
	 * @param sequenceID - sequence ID, lower is newer.
	 */
	public void add(HaeinsaKeyValue kv, long sequenceID) {
		switch (kv.getType()) {
		case DeleteFamily: {
			Long previous = families.get(kv.getFamily());
			if (previous == null || previous.compareTo(sequenceID) > 0) {
				// sequenceId is lower than previous one.
				families.put(kv.getFamily(), sequenceID);
			}
			break;
		}
		case DeleteColumn: {
			NavigableMap<byte[], Long> cellMap = cells.get(kv.getFamily());
			if (cellMap == null) {
				cellMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				cells.put(kv.getFamily(), cellMap);
			}
			Long previous = families.get(kv.getQualifier());
			if (previous == null || previous.compareTo(sequenceID) > 0) {
				// sequenceId is lower than previous one.
				cellMap.put(kv.getQualifier(), sequenceID);
			}
			break;
		}
		default:
			break;
		}
	}

	/**
	 *
	 * @param kv
	 * @param sequenceID
	 * @return Return true if kv is deleted after sequenceID (lower sequenceID),
	 *         return false otherwise.
	 */
	public boolean isDeleted(HaeinsaKeyValue kv, long sequenceID) {
		// check family
		Long deletedSequenceID = families.get(kv.getFamily());
		if (deletedSequenceID != null && deletedSequenceID.compareTo(sequenceID) < 0) {
			return true;
		}

		// check cell
		NavigableMap<byte[], Long> cellMap = cells.get(kv.getFamily());
		if (cellMap != null) {
			deletedSequenceID = cellMap.get(kv.getQualifier());
			if (deletedSequenceID != null && deletedSequenceID.compareTo(sequenceID) < 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * clear inside family & column tracker
	 */
	public void reset() {
		families.clear();
		cells.clear();
	}
}
