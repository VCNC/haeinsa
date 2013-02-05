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
	private final NavigableMap<byte[], TableTransaction> tableStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final TransactionManager manager;
	private TRowKey primary;
	private long commitTimestamp = Long.MIN_VALUE;
	private long prewriteTimestamp = Long.MIN_VALUE;
	
	public Transaction(TransactionManager manager){
		this.manager = manager;
	}
	
	public NavigableMap<byte[], TableTransaction> getTableStates() {
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
	
	public TableTransaction createOrGetTableState(byte[] tableName){
		TableTransaction tableTxState = tableStates.get(tableName);
		if (tableTxState == null){
			tableTxState = new TableTransaction(this);
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
		RowTransaction primaryRowState = null;
		long commitTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		long prewriteTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		for (Entry<byte[], TableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if (primaryRowKey == null){
					primaryRowKey = new TRowKey();
					primaryRowKey.setTableName(tableStateEntry.getKey());
					primaryRowKey.setRow(rowStateEntry.getKey());
					primaryRowState = rowStateEntry.getValue();
				}
				RowTransaction rowState = rowStateEntry.getValue();
				commitTimestamp = Math.max(commitTimestamp, rowState.getCurrent().getCommitTimestamp() + rowState.getIterationCount());
				prewriteTimestamp = Math.max(prewriteTimestamp, rowState.getCurrent().getCommitTimestamp() + 1);
			}
		}
		commitTimestamp = Math.max(commitTimestamp, prewriteTimestamp);
		setPrimary(primaryRowKey);
		setCommitTimestamp(commitTimestamp);
		setPrewriteTimestamp(prewriteTimestamp);
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// prewrite primary row
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primaryRowKey.getTableName());
			table.prewrite(primaryRowState, primaryRowKey.getRow(), true);
		}
		
		// prewrite secondaries
		for (Entry<byte[], TableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if ((Bytes.equals(tableStateEntry.getKey(), primaryRowKey.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primaryRowKey.getRow()))){
					continue;
				}
				HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableStateEntry.getKey());
				table.prewrite(rowStateEntry.getValue(), rowStateEntry.getKey(), false);
			}
		}
		
		makeStable();
	}
	
	private void makeStable() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		RowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		// commit primary or get more time to commit this.
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.commitPrimary(primaryRowTx, primary.getRow());
		}
		
		for (Entry<byte[], TableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				// apply mutations  
				HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableStateEntry.getKey());
				table.applyMutations(rowStateEntry.getValue(), rowStateEntry.getKey());
				
				if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
					continue;
				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
			}
		}
		
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.makeStable(primaryRowTx, primary.getRow());
		}
	}
	
	public void recover() throws IOException {
		RowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		if (primaryRowTx.getCurrent().getState() == TRowLockState.PREWRITTEN){
			// prewritten 상태에서는 timeout 보다 primary이 시간이 더 지났으면 abort 시켜야 함.
			if (primaryRowTx.getCurrent().getTimeout() < System.currentTimeMillis()){
				
			}else{
				// timeout이 지나지 않았다면, recover를 실패시켜야 함.
				throw new ConflictException();
			}
		}
		
		switch (primaryRowTx.getCurrent().getState()) {
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
		RowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		{
			// abort primary row
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.abortPrimary(primaryRowTx, primary.getRow());
		}
		
		for (Entry<byte[], TableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], RowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				// delete prewritten  
				HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableStateEntry.getKey());
				table.deletePrewritten(rowStateEntry.getValue(), rowStateEntry.getKey());
				
				if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
					continue;
				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
			}
		}
		
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.makeStable(primaryRowTx, primary.getRow());
		}

	}
}
