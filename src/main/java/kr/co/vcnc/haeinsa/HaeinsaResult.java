package kr.co.vcnc.haeinsa;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

/**
 * Modified POJO container of {@link Result} class in HBase.
 * Link {@link Result}, can contain multiple {@link HaeinsaKeyValue}.
 * All HaeinsaKeyValue in HaeinsaResult are from same row.
 * 
 * @author Myungbo Kim
 *
 */
public class HaeinsaResult {
	private final List<HaeinsaKeyValue> sortedKVs;
	private byte[] row = null;

	/**
	 * Construct HaeinsaResultImpl from sorted list of HaeinsaKeyValue
	 * @param sortedKVs - assume all the HaeinsaKeyValue in sortedKVs have same row and sorted in ascending order.
	 */
	public HaeinsaResult(List<HaeinsaKeyValue> sortedKVs) {
		this.sortedKVs = sortedKVs;
		if (sortedKVs.size() > 0) {
			row = sortedKVs.get(0).getRow();
		}
	}

	public byte[] getRow() {
		return row;
	}

	public List<HaeinsaKeyValue> list() {
		return sortedKVs;
	}

	public byte[] getValue(byte[] family, byte[] qualifier) {
		int index = Collections.binarySearch(sortedKVs, new HaeinsaKeyValue(row,
				family, qualifier, null, KeyValue.Type.Maximum),
				HaeinsaKeyValue.COMPARATOR);
		if (index >= 0) {
			return sortedKVs.get(index).getValue();
		}
		return null;
	}

	public boolean containsColumn(byte[] family, byte[] qualifier) {
		return getValue(family, qualifier) != null;
	}

	public boolean isEmpty() {
		return sortedKVs.size() == 0;
	}
}
