package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_MIN_TIMESTAMP;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

public class Transaction {
	private final NavigableMap<byte[], TableTransactionState> tableStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final TransactionManager manager;
	private TRowKey primary;
	private long commitTimestamp = Long.MIN_VALUE;
	private long prewriteTimestamp = Long.MIN_VALUE;
	
	public Transaction(TransactionManager manager){
		this.manager = manager;
	}
	
	public NavigableMap<byte[], TableTransactionState> getTableStates() {
		return tableStates;
	}
	
	public TransactionManager getManager() {
		return manager;
	}
	
	public long getPrewriteTimestamp() {
		return prewriteTimestamp;
	}
	
	void setPrewriteTimestamp(long startTimestamp) {
		this.prewriteTimestamp = startTimestamp;
	}
	
	public long getCommitTimestamp() {
		return commitTimestamp;
	}
	
	void setCommitTimestamp(long commitTimestamp) {
		this.commitTimestamp = commitTimestamp;
	}
	
	public TRowKey getPrimary() {
		return primary;
	}
	
	void setPrimary(TRowKey primary) {
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
		TRowKey primaryRowKey = null;
		RowTransactionState primaryRowState = null;
		long commitTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		long prewriteTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		for (Entry<byte[], TableTransactionState> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if (primaryRowKey == null){
					primaryRowKey = new TRowKey();
					primaryRowKey.setTableName(tableStateEntry.getKey());
					primaryRowKey.setRow(rowStateEntry.getKey());
					primaryRowState = rowStateEntry.getValue();
				}
				RowTransactionState rowState = rowStateEntry.getValue();
				commitTimestamp = Math.max(commitTimestamp, rowState.getCurrentRowLock().getCommitTimestamp() + rowState.getMutations().size());
				prewriteTimestamp = Math.max(prewriteTimestamp, rowState.getCurrentRowLock().getCommitTimestamp() + 1);
			}
		}
		commitTimestamp = Math.max(commitTimestamp, prewriteTimestamp);
		setPrimary(primaryRowKey);
		setCommitTimestamp(commitTimestamp);
		setPrewriteTimestamp(prewriteTimestamp);
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// prewrite primary row
		{
			HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(primaryRowKey.getTableName());
			table.prewrite(primaryRowState, primaryRowKey.getRow(), true);
		}
		
		// prewrite secondaries
		for (Entry<byte[], TableTransactionState> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if ((Bytes.equals(tableStateEntry.getKey(), primaryRowKey.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primaryRowKey.getRow()))){
					continue;
				}
				HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(tableStateEntry.getKey());
				table.prewrite(rowStateEntry.getValue(), rowStateEntry.getKey(), false);
			}
		}
		
		makeStable();
	}
	
	private void makeStable() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		RowTransactionState primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		// commit primary or get more time to commit this.
		{
			HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(primary.getTableName());
			table.commitPrimary(primaryRowTx, primary.getRow());
		}
		
		for (Entry<byte[], TableTransactionState> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				// apply mutations  
				HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(tableStateEntry.getKey());
				table.applyMutations(rowStateEntry.getValue(), rowStateEntry.getKey());
				
				if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
					continue;
				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
			}
		}
		
		{
			HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(primary.getTableName());
			table.makeStable(primaryRowTx, primary.getRow());
		}
	}
	
	public void recover() throws IOException {
		RowTransactionState primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		if (primaryRowTx.getCurrentRowLock().getState() == TRowLockState.PREWRITTEN){
			// prewritten 상태에서는 timeout 보다 primary이 시간이 더 지났으면 abort 시켜야 함.
			if (primaryRowTx.getCurrentRowLock().getTimeout() < System.currentTimeMillis()){
				
			}else{
				// timeout이 지나지 않았다면, recover를 실패시켜야 함.
				throw new ConflictException();
			}
		}
		
		switch (primaryRowTx.getCurrentRowLock().getState()) {
		case ABORTED:
		case PREWRITTEN:{
			abort();
			break;
		}
		
		case COMMITTED:{
			makeStable();
			break;
		}

		default:
			throw new ConflictException();
		}
	}

	public void abort() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		RowTransactionState primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		{
			// abort primary row
			HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(primary.getTableName());
			table.abortPrimary(primaryRowTx, primary.getRow());
		}
		
		for (Entry<byte[], TableTransactionState> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				// delete prewritten  
				HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(tableStateEntry.getKey());
				table.deletePrewritten(rowStateEntry.getValue(), rowStateEntry.getKey());
				
				if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
					continue;
				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
			}
		}
		
		{
			HaeinsaTableInterface.Private table = (HaeinsaTableInterface.Private) tablePool.getTable(primary.getTableName());
			table.makeStable(primaryRowTx, primary.getRow());
		}

	}
}
