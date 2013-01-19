package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.Constants.ROW_LOCK_MIN_TIMESTAMP;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import kr.co.vcnc.haeinsa.thrift.RowKey;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

public class Transaction {
	private final NavigableMap<byte[], TableTransactionState> tableStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final TransactionManager manager;
	private RowKey primary;
	private long commitTimestamp = Long.MIN_VALUE;
	
	public Transaction(TransactionManager manager){
		this.manager = manager;
	}
	
	public NavigableMap<byte[], TableTransactionState> getTableStates() {
		return tableStates;
	}
	
	public TransactionManager getManager() {
		return manager;
	}
	
	public long getCommitTimestamp() {
		return commitTimestamp;
	}
	
	void setCommitTimestamp(long commitTimestamp) {
		this.commitTimestamp = commitTimestamp;
	}
	
	public RowKey getPrimary() {
		return primary;
	}
	
	void setPrimary(RowKey primary) {
		this.primary = primary;
	}
	
	public TableTransactionState createOrGetTableState(byte[] tableName){
		TableTransactionState tableTxState = tableStates.get(tableName);
		if (tableTxState == null){
			tableTxState = new TableTransactionState(this);
			tableStates.put(tableName, tableTxState);
		}
		return tableTxState;
	}
	
	public void rollback() throws IOException {
		// do nothing
	}
	
	public void commit() throws IOException {
		// determine commitTimestamp & determine primary row
		RowKey primaryRowKey = null;
		RowTransactionState primaryRowState = null;
		long commitTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		for (Entry<byte[], TableTransactionState> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if (primaryRowKey == null){
					primaryRowKey = new RowKey();
					primaryRowKey.setTableName(tableStateEntry.getKey());
					primaryRowKey.setRow(rowStateEntry.getKey());
					primaryRowState = rowStateEntry.getValue();
				}
				commitTimestamp = Math.max(commitTimestamp, rowStateEntry.getValue().getCurrentRowLock().getCommitTimestamp() + 1);
			}
		}
		setPrimary(primaryRowKey);
		setCommitTimestamp(commitTimestamp);
		
		TablePool tablePool = getManager().getTablePool();
		// prewrite primary row
		{
			Table.PrivateIface table = (Table.PrivateIface) tablePool.getTable(primaryRowKey.getTableName());
			table.prewrite(primaryRowState, primaryRowKey.getRow(), true);
		}
		
		// prewrite secondaries
		for (Entry<byte[], TableTransactionState> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if ((Bytes.equals(tableStateEntry.getKey(), primaryRowKey.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primaryRowKey.getRow()))){
					continue;
				}
				Table.PrivateIface table = (Table.PrivateIface) tablePool.getTable(tableStateEntry.getKey());
				table.prewrite(rowStateEntry.getValue(), rowStateEntry.getKey(), false);
			}
		}
		
		// commit primary
		{
			Table.PrivateIface table = (Table.PrivateIface) tablePool.getTable(primaryRowKey.getTableName());
			table.commitPrimary(primaryRowState, primaryRowKey.getRow());
		}
		
		
		for (Entry<byte[], TableTransactionState> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				// apply deletes  
				Table.PrivateIface table = (Table.PrivateIface) tablePool.getTable(tableStateEntry.getKey());
				table.applyDeletes(rowStateEntry.getValue(), rowStateEntry.getKey());
				
				if ((Bytes.equals(tableStateEntry.getKey(), primaryRowKey.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primaryRowKey.getRow()))){
					continue;
				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
			}
		}
		
		{
			Table.PrivateIface table = (Table.PrivateIface) tablePool.getTable(primaryRowKey.getTableName());
			table.makeStable(primaryRowState, primaryRowKey.getRow());
		}
	}
	
	public void recover() throws IOException {
		
	}
}
