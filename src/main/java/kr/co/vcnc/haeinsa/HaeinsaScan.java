package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HaeinsaScan can be analogous to {@link Scan} class in HBase. 
 * <p>HaeinsaScan do not support to set batch value, but only support caching.
 * So if user only specify column family and retrieve data from HBase, 
 * {@link HaeinsaResultScanner} will return whole column family of the row at one time. 
 * <p>Setting batch size will be supported in future....(?) 
 * @author Myungbo Kim
 *
 */
public class HaeinsaScan {
	private byte[] startRow = HConstants.EMPTY_START_ROW;
	private byte[] stopRow = HConstants.EMPTY_END_ROW;
	private int caching = -1;
	private boolean cacheBlocks = true;

	private Map<byte[], NavigableSet<byte[]>> familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(
			Bytes.BYTES_COMPARATOR);
	
	public HaeinsaScan(){
	}

	/**
	 * Create a Scan operation starting at the specified row.
	 * <p>
	 * If the specified row does not exist, the Scanner will start from the next
	 * closest row after the specified row.
	 * 
	 * @param startRow
	 *            row to start scanner at or after
	 */
	public HaeinsaScan(byte[] startRow) {
		this.startRow = startRow;
	}

	/**
	 * Create a Scan operation for the range of rows specified.
	 * 
	 * @param startRow
	 *            row to start scanner at or after (inclusive)
	 * @param stopRow
	 *            row to stop scanner before (exclusive)
	 */
	public HaeinsaScan(byte[] startRow, byte[] stopRow) {
		this.startRow = startRow;
		this.stopRow = stopRow;
	}

	/**
	 * Creates a new instance of this class while copying all values.
	 * 
	 * @param scan
	 *            The scan instance to copy from.
	 * @throws IOException
	 *             When copying the values fails.
	 */
	public HaeinsaScan(HaeinsaScan scan) throws IOException {
		startRow = scan.getStartRow();
		stopRow = scan.getStopRow();
		caching = scan.getCaching();
		cacheBlocks = scan.getCacheBlocks();
		Map<byte[], NavigableSet<byte[]>> fams = scan.getFamilyMap();
		for (Map.Entry<byte[], NavigableSet<byte[]>> entry : fams.entrySet()) {
			byte[] fam = entry.getKey();
			NavigableSet<byte[]> cols = entry.getValue();
			if (cols != null && cols.size() > 0) {
				for (byte[] col : cols) {
					addColumn(fam, col);
				}
			} else {
				addFamily(fam);
			}
		}
	}

	/**
	 * Get all columns from the specified family.
	 * <p>
	 * Overrides previous calls to addColumn for this family.
	 * 
	 * @param family
	 *            family name
	 * @return this
	 */
	public HaeinsaScan addFamily(byte[] family) {
		familyMap.remove(family);
		familyMap.put(family, null);
		return this;
	}

	/**
	 * Get the column from the specified family with the specified qualifier.
	 * <p>
	 * Overrides previous calls to addFamily for this family.
	 * 
	 * @param family
	 *            family name
	 * @param qualifier
	 *            column qualifier
	 * @return this
	 */
	public HaeinsaScan addColumn(byte[] family, byte[] qualifier) {
		NavigableSet<byte[]> set = familyMap.get(family);
		if (set == null) {
			set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		}
		set.add(qualifier);
		familyMap.put(family, set);

		return this;
	}

	/**
	 * Set the start row of the scan.
	 * 
	 * @param startRow
	 *            row to start scan on, inclusive
	 * @return this
	 */
	public HaeinsaScan setStartRow(byte[] startRow) {
		this.startRow = startRow;
		return this;
	}

	/**
	 * Set the stop row.
	 * 
	 * @param stopRow
	 *            row to end at (exclusive)
	 * @return this
	 */
	public HaeinsaScan setStopRow(byte[] stopRow) {
		this.stopRow = stopRow;
		return this;
	}

	/**
	 * Set the number of rows for caching that will be passed to scanners. If
	 * not set, the default setting from {@link HTable#getScannerCaching()} will
	 * apply. Higher caching values will enable faster scanners but will use
	 * more memory.
	 * 
	 * @param caching
	 *            the number of rows for caching
	 */
	public void setCaching(int caching) {
		this.caching = caching;
	}

	/**
	 * Setting the familyMap
	 * 
	 * @param familyMap
	 *            map of family to qualifier
	 * @return this
	 */
	public HaeinsaScan setFamilyMap(Map<byte[], NavigableSet<byte[]>> familyMap) {
		this.familyMap = familyMap;
		return this;
	}

	/**
	 * Getting the familyMap
	 * 
	 * @return familyMap
	 */
	public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
		return this.familyMap;
	}

	/**
	 * @return the number of families in familyMap
	 */
	public int numFamilies() {
		if (hasFamilies()) {
			return this.familyMap.size();
		}
		return 0;
	}

	/**
	 * @return true if familyMap is non empty, false otherwise
	 */
	public boolean hasFamilies() {
		return !this.familyMap.isEmpty();
	}

	/**
	 * @return the keys of the familyMap
	 */
	public byte[][] getFamilies() {
		if (hasFamilies()) {
			return this.familyMap.keySet().toArray(new byte[0][0]);
		}
		return null;
	}

	/**
	 * @return the startrow
	 */
	public byte[] getStartRow() {
		return this.startRow;
	}

	/**
	 * @return the stoprow
	 */
	public byte[] getStopRow() {
		return this.stopRow;
	}

	/**
	 * @return caching the number of rows fetched when calling next on a scanner
	 */
	public int getCaching() {
		return this.caching;
	}

	/**
	 * Set whether blocks should be cached for this Scan.
	 * HBase 에서 해당 Scan 을 위해서 HFile 의 Block 을 memory 에 Cache 하고 있을 것인지를 결정할 수 있다.
	 * 일반적으로 Block 을 caching 하면 다음번 get request 에 대한 응답이 빠를 수 있으나 DB 의 메모리 소모가 심해진다.
	 * cacheBlocks 와 caching 은 서로 관계가 없는 설정이다.
	 * <p>
	 * This is true by default. When true, default settings of the table and
	 * family are used (this will never override caching blocks if the block
	 * cache is disabled for that family or entirely).
	 * 
	 * @param cacheBlocks
	 *            if false, default settings are overridden and blocks will not
	 *            be cached
	 */
	public void setCacheBlocks(boolean cacheBlocks) {
		this.cacheBlocks = cacheBlocks;
	}

	/**
	 * Get whether blocks should be cached for this Scan.
	 * 
	 * @return true if default setting of block caching should be used, false if blocks should
	 *         not be cached
	 */
	public boolean getCacheBlocks() {
		return cacheBlocks;
	}

}
