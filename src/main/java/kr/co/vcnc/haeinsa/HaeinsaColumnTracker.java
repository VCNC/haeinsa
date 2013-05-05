package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Sets;

/**
 * Tracking parameters of {@link HaeinsaScan} and {@link HaeinsaIntraScan} inside {@link HaeinsaTable}.
 * <p>Tracker can determine whether specific HaeinsaKeyValue inside scan range.
 */
public class HaeinsaColumnTracker {
	//	{ family -> qualifier }
	private final Map<byte[], NavigableSet<byte[]>> familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(
			Bytes.BYTES_COMPARATOR);

	private final byte[] minColumn;
	private final boolean minColumnInclusive;
	private final byte[] maxColumn;
	private final boolean maxColumnInclusive;

	/**
	 * Constructor of HaeinsaColumnTracker.
	 *
	 * <p>If this ColumnTracker track {@link HaeinsaScan}, minColumn, maxColumn should be null and
	 * minColumnInclusive, maxColumnInclusive should be false.
	 * @param familyMap
	 *
	 * @param minColumn
	 * @param minColumnInclusive
	 * @param maxColumn
	 * @param maxColumnInclusive
	 */
	public HaeinsaColumnTracker(Map<byte[], NavigableSet<byte[]>> familyMap,
			byte[] minColumn, boolean minColumnInclusive, byte[] maxColumn,
			boolean maxColumnInclusive) {
		this.minColumn = minColumn;
		this.maxColumn = maxColumn;
		this.minColumnInclusive = minColumnInclusive;
		this.maxColumnInclusive = maxColumnInclusive;
		for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
			if (entry.getValue() != null) {
				NavigableSet<byte[]> qualifierSet = Sets
						.newTreeSet(Bytes.BYTES_COMPARATOR);
				qualifierSet.addAll(entry.getValue());
				this.familyMap.put(entry.getKey(), qualifierSet);
			} else {
				this.familyMap.put(entry.getKey(), null);
			}
		}
	}

	/**
	 * Return true if qualifier of kv is placed between minColumn and maxColumn.
	 * <p>Using lexicographical ordering to compare byte[]
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

		//	kv 의 column 이 minColumn 값이 정하는 requirement 를 만족시키고 있으면서 maxColumn requirement 는 없을 때
		if (this.maxColumn == null) {
			return true;
		}

		int cmpMax = Bytes.compareTo(kv.getQualifier(), maxColumn);

		if (this.maxColumnInclusive && cmpMax <= 0 ||
				!this.maxColumnInclusive && cmpMax < 0) {
			return true;
		}

		return false;
	}

	/**
	 * Argument 로 넘긴 HaeinsaKeyValue 가 scan 범위 안에 포함되는 지 판별해 준다. 다음 세 가지 경우가 있을 수 있다.
	 *
	 * <p>1. Scan 에 family 가 전혀 지정되지 않은 경우 - qualifier 범위에만 포함되면 된다.
	 *
	 * <p>2. Scan 에 family 가 지정되어 있고, kv 의 family 와 같지만 qualifier 는 지정되어 있지 않은 경우 - kv 와 family 가 같고
	 * qualifier 가 Scan 범위 안에 포함되면 된다.
	 *
	 * <p>3. Scan 에 family 와 qualifier 가 함께 지정되어 있는 경우 - kv 의 (family, qualifier) 가 같아야 한다.
	 * @param kv
	 * @return
	 */
	public boolean isMatched(HaeinsaKeyValue kv) {
		//	familyMap 이 비어있다는 것은 scan 시에 특정 family 를 지정하지 않고 모든 (family, qualifier)를 scan 하는 것이라고 가정하여,
		//	isColumnInclusive(kv) 를 바로 호출한다.
		//	{ empty }
		if (familyMap.isEmpty()) {
			return isColumnInclusive(kv);
		}

		NavigableSet<byte[]> set = familyMap.get(kv.getFamily());
		//	해당 family 는 설정 되어 있고, qualifier 는 설정되어 있지 않은 경우
		//	{ family -> null }
		if (familyMap.containsKey(kv.getFamily()) && set == null) {
			return isColumnInclusive(kv);
		}

		//	해당 family 가 familyMap 에 있고 qualifier 도 설정되어 있는 경우
		//	{ family -> qualifier }
		if (set.contains(kv.getQualifier())) {
			return isColumnInclusive(kv);
		}
		return false;
	}
}
