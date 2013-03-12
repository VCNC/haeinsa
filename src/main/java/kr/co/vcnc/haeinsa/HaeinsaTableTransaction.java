package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Contains Transaction information of single Table.
 * <p>It have map of {byte[] row -> {@link HaeinsaRowTransaction}} and reference to {@link HaeinsaTransaction} 
 * @author Myungbo Kim
 *
 */
class HaeinsaTableTransaction {
	private final NavigableMap<byte[], HaeinsaRowTransaction> rowStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final HaeinsaTransaction transaction;
	
	HaeinsaTableTransaction(HaeinsaTransaction transaction){
		this.transaction = transaction;
	}
	
	public NavigableMap<byte[], HaeinsaRowTransaction> getRowStates() {
		return rowStates;
	}
	
	public HaeinsaTransaction getTransaction() {
		return transaction;
	}
	
	/**
	 * 현재의 TableTransaction 이 들고 있는 RowTransaction 을 return 한다.
	 * 만약 주어진 row 에 해당하는 RowTransaction 이 존재하지 않는다면, TableTransaction 내에 해당 row 의 RowTransaction 을 만들고 
	 * 그 RowTransaction 을 내부 map 에 저장한 후에 return 한다. 
	 * @param row
	 * @return RowTransaction - {@link HaeinsaRowTransaction} which contained in this instance.
	 */
	public HaeinsaRowTransaction createOrGetRowState(byte[] row) {
		HaeinsaRowTransaction rowState = rowStates.get(row);
		if (rowState == null){
			rowState = new HaeinsaRowTransaction(this);
			rowStates.put(row, rowState);
		}
		return rowState;
	}
}
