package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

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
	
	public RowTransaction createOrGetRowState(byte[] row) {
		RowTransaction rowState = rowStates.get(row);
		if (rowState == null){
			rowState = new RowTransaction(this);
			rowStates.put(row, rowState);
		}
		return rowState;
	}
}
