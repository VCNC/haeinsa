package kr.co.vcnc.haeinsa;

import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Custom integration of {@link ColumnRangeFilter} in Haeinsa.
 * In contrast to {@link HaeinsaScan}, HaeinsaIntraScan can be used to retrieve 
 * range of column qualifier inside single row with scan-like way.
 * 
 * <p>User can specify column family, range of qualifier, size of batch at a time and 
 * whether start column and last column are included. 
 * 
 * <p>Default batch size is 32.
 * @author Myungbo Kim
 *
 */
public class HaeinsaIntraScan {
	private final byte[] row;
	private final byte[] minColumn;
	private final boolean minColumnInclusive;
	private final byte[] maxColumn;
	private final boolean maxColumnInclusive;
	private int batch = 32;
	
	//	if this set is empty, then scan every family
	private final NavigableSet<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

	public HaeinsaIntraScan(final byte[] row, final byte[] minColumn,
			boolean minColumnInclusive, final byte[] maxColumn,
			boolean maxColumnInclusive) {
		this.row = row;
		this.minColumn = minColumn;
		this.minColumnInclusive = minColumnInclusive;
		this.maxColumn = maxColumn;
		this.maxColumnInclusive = maxColumnInclusive;
	}
	
	public byte[] getMaxColumn() {
		return maxColumn;
	}
	
	public byte[] getMinColumn() {
		return minColumn;
	}
	
	public byte[] getRow() {
		return row;
	}
	
	public boolean isMinColumnInclusive() {
		return minColumnInclusive;
	}
	
	public boolean isMaxColumnInclusive() {
		return maxColumnInclusive;
	}
	
	public HaeinsaIntraScan addFamily(byte[] family){
		families.add(family);
		return this;
	}
	
	public void setBatch(int batch) {
		this.batch = batch;
	}
	
	public int getBatch() {
		return batch;
	}
	
	public NavigableSet<byte[]> getFamilies() {
		return families;
	}
}
