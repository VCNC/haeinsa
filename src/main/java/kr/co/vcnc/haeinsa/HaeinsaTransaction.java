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

public class HaeinsaTransaction {
	private final NavigableMap<byte[], HaeinsaTableTransaction> tableStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final HaeinsaTransactionManager manager;
	private TRowKey primary;
	private long commitTimestamp = Long.MIN_VALUE;
	private long prewriteTimestamp = Long.MIN_VALUE;
	private static enum CommitMethod {
		SINGLE_ROW_PUT_ONLY,
		SINGLE_ROW_READ_ONLY,
		MULTI_ROW
	}
	
	public HaeinsaTransaction(HaeinsaTransactionManager manager){
		this.manager = manager;
	}
	
	protected NavigableMap<byte[], HaeinsaTableTransaction> getTableStates() {
		return tableStates;
	}
	
	public HaeinsaTransactionManager getManager() {
		return manager;
	}
	
	public long getPrewriteTimestamp() {
		return prewriteTimestamp;
	}
	
	protected void setPrewriteTimestamp(long prewriteTimestamp) {
		this.prewriteTimestamp = prewriteTimestamp;
	}
	
	public long getCommitTimestamp() {
		return commitTimestamp;
	}
	
	protected void setCommitTimestamp(long commitTimestamp) {
		this.commitTimestamp = commitTimestamp;
	}
	
	public TRowKey getPrimary() {
		return primary;
	}
	
	protected void setPrimary(TRowKey primary) {
		this.primary = primary;
	}
	
	protected HaeinsaTableTransaction createOrGetTableState(byte[] tableName){
		HaeinsaTableTransaction tableTxState = tableStates.get(tableName);
		if (tableTxState == null){
			tableTxState = new HaeinsaTableTransaction(this);
			tableStates.put(tableName, tableTxState);
		}
		return tableTxState;
	}
	
	public void rollback() throws IOException {
		// do nothing
	}
	
	protected CommitMethod determineCommitMethod(){
		int count = 0;
		CommitMethod method = CommitMethod.SINGLE_ROW_READ_ONLY;
		for (HaeinsaTableTransaction tableState : getTableStates().values()){
			for (HaeinsaRowTransaction rowState : tableState.getRowStates().values()){
				count ++;
				if (count > 1) {
					return CommitMethod.MULTI_ROW;
				}
				if (rowState.getMutations().size() <= 0){
					method = CommitMethod.SINGLE_ROW_READ_ONLY;
				}else if (rowState.getMutations().get(0) instanceof HaeinsaPut && rowState.getMutations().size() == 1) {
					method = CommitMethod.SINGLE_ROW_PUT_ONLY;
				}else {
					method = CommitMethod.MULTI_ROW;
				}
			}
		}
		return method;
	}
	
	protected void commitMultiRows() throws IOException{		
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// prewrite primary row
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.prewrite(primaryRowState, primary.getRow(), true);
		}
		
		// prewrite secondaries
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
					continue;
				}
				HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableStateEntry.getKey());
				table.prewrite(rowStateEntry.getValue(), rowStateEntry.getKey(), false);
			}
		}
		
		makeStable();
	}
	
	protected void commitSingleRowPutOnly() throws IOException {
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// commit primary row
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.commitSingleRowPutOnly(primaryRowState, primary.getRow());
		}
	}
	
	protected void commitSingleRowReadOnly() throws IOException {
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// commit primary row
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.commitSingleRowReadOnly(primaryRowState, primary.getRow());
		}
	}
		
	public void commit() throws IOException { 
		// determine commitTimestamp & determine primary row
		TRowKey primaryRowKey = null;
		long commitTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		long prewriteTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if (primaryRowKey == null){
					primaryRowKey = new TRowKey();
					primaryRowKey.setTableName(tableStateEntry.getKey());
					primaryRowKey.setRow(rowStateEntry.getKey());
				}
				HaeinsaRowTransaction rowState = rowStateEntry.getValue();
				commitTimestamp = Math.max(commitTimestamp, rowState.getCurrent().getCommitTimestamp() + rowState.getIterationCount());
				prewriteTimestamp = Math.max(prewriteTimestamp, rowState.getCurrent().getCommitTimestamp() + 1);
			}
		}
		commitTimestamp = Math.max(commitTimestamp, prewriteTimestamp);
		setPrimary(primaryRowKey);
		setCommitTimestamp(commitTimestamp);
		setPrewriteTimestamp(prewriteTimestamp);
		
		CommitMethod method = determineCommitMethod();
		switch (method) {
		case MULTI_ROW:{
			commitMultiRows();
			break;
		}
		case SINGLE_ROW_READ_ONLY:{
			commitSingleRowReadOnly();
			break;
		}
		
		case SINGLE_ROW_PUT_ONLY:{
			commitSingleRowPutOnly();
			break;
		}

		default:
			break;
		}
		
	}
	
	private void makeStable() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		// commit primary or get more time to commit this.
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.commitPrimary(primaryRowTx, primary.getRow());
		}
		
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
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
	
	protected void recover() throws IOException {
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		if (primaryRowTx.getCurrent().getState() == TRowLockState.PREWRITTEN){
			// prewritten 상태에서는 timeout 보다 primary이 시간이 더 지났으면 abort 시켜야 함.
			if (primaryRowTx.getCurrent().getExpiry() < System.currentTimeMillis()){
				
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

	protected void abort() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		{
			// abort primary row
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			table.abortPrimary(primaryRowTx, primary.getRow());
		}
		
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
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
