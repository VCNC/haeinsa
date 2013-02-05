package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Sets;

public class HaeinsaColumnTracker {
	private final Map<byte[], NavigableSet<byte[]>> familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(
			Bytes.BYTES_COMPARATOR);

	private final byte[] minColumn;
	private final boolean minColumnInclusive;
	private final byte[] maxColumn;
	private final boolean maxColumnInclusive;

	public HaeinsaColumnTracker(Map<byte[], NavigableSet<byte[]>> familyMap,
			byte[] minColumn, boolean minColumnInclusive, byte[] maxColumn,
			boolean maxColumnInclusive) {
		this.minColumn = minColumn;
		this.maxColumn = maxColumn;
		this.minColumnInclusive = minColumnInclusive;
		this.maxColumnInclusive = maxColumnInclusive;
		for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
			if (entry.getValue() != null) {
				NavigableSet<byte[]> set = Sets
						.newTreeSet(Bytes.BYTES_COMPARATOR);
				set.addAll(entry.getValue());
				this.familyMap.put(entry.getKey(), set);
			} else {
				this.familyMap.put(entry.getKey(), null);
			}
		}
	}

	public boolean isColumnInclusive(HaeinsaKeyValue kv) {
		int cmpMin = 1;
		if (this.minColumn != null){
			cmpMin = Bytes.compareTo(kv.getQualifier(), minColumn);
		}
		
		if (cmpMin < 0){
			return false;
		}
		
		if (!this.minColumnInclusive && cmpMin == 0){
			return false;
		}
		
		if (this.maxColumn == null){
			return true;
		}
		
		int cmpMax = Bytes.compareTo(kv.getQualifier(), maxColumn);
		
		if (this.maxColumnInclusive && cmpMax <=0 ||
				!this.maxColumnInclusive && cmpMax < 0) {
			return true;
		}

		return false;
	}

	public boolean isMatched(HaeinsaKeyValue kv) {
		if (familyMap.isEmpty()) {
			return isColumnInclusive(kv);
		}
		NavigableSet<byte[]> set = familyMap.get(kv.getFamily());
		if (set == null) {
			return isColumnInclusive(kv);
		}
		if (set.contains(kv.getQualifier())) {
			return isColumnInclusive(kv);
		}

		return false;
	}
}
