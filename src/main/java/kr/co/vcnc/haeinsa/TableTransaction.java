package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Contains Transaction information of single Table.
 * <p>It have map of {byte[] row -> {@link RowTransaction}} and reference to {@link Transaction} 
 * @author Myungbo Kim
 *
 */
public class TableTransaction {
	private final NavigableMap<byte[], RowTransaction> rowStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final Transaction transaction;
	
	TableTransaction(Transaction transaction){
		this.transaction = transaction;
	}
	
	public NavigableMap<byte[], RowTransaction> getRowStates() {
		return rowStates;
	}
	
	public Transaction getTransaction() {
		return transaction;
	}
	
	/**
	 * 현재의 TableTransaction 이 들고 있는 RowTransaction 을 return 한다.
	 * 만약 주어진 row 에 해당하는 RowTransaction 이 존재하지 않는다면, TableTransaction 내에 해당 row 의 RowTransaction 을 만들고 
	 * 그 RowTransaction 을 내부 map 에 저장한 후에 return 한다. 
	 * @param row
	 * @return RowTransaction - {@link RowTransaction} which contained in this instance.
	 */
	public RowTransaction createOrGetRowState(byte[] row) {
		RowTransaction rowState = rowStates.get(row);
		if (rowState == null){
			rowState = new RowTransaction(this);
			rowStates.put(row, rowState);
		}
		return rowState;
	}
}
