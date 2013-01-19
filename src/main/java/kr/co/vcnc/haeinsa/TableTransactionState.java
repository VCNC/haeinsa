package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

public class TableTransactionState {
	private final NavigableMap<byte[], RowTransactionState> rowStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final Transaction transaction;
	
	TableTransactionState(Transaction transaction){
		this.transaction = transaction;
	}
	
	public NavigableMap<byte[], RowTransactionState> getRowStates() {
		return rowStates;
	}
	
	public Transaction getTransaction() {
		return transaction;
	}
	
	public RowTransactionState createOrGetRowState(byte[] row) {
		RowTransactionState rowState = rowStates.get(row);
		if (rowState == null){
			rowState = new RowTransactionState(this);
			rowStates.put(row, rowState);
		}
		return rowState;
	}
}
