package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_MIN_TIMESTAMP;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.NavigableMap;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * TODO
 * @author Myungbo Kim
 *
 */
public class HaeinsaTransaction {
	private final NavigableMap<byte[], HaeinsaTableTransaction> tableStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final HaeinsaTransactionManager manager;
	private TRowKey primary;
	private long commitTimestamp = Long.MIN_VALUE;
	private long prewriteTimestamp = Long.MIN_VALUE;
	private final AtomicBoolean used = new AtomicBoolean(false);
	private static enum CommitMethod {
		SINGLE_ROW_PUT_ONLY,	//	rowTx 가 하나만 존재하고, muation 의 종류가 HaeinsaPut 일 때 
		SINGLE_ROW_READ_ONLY,	//	rowTx 가 하나만 존재하고, muations 가 없을 때 
		MULTI_ROW				//	rowTx 가 여러 개 존재하거나, rowTx 가 하나만 존재하면서 종류가 HaeinsaDelete 이거나 
								//	rowTx 가 하나만 존재하면서 여러 개의 HaeinsaPut / HeainsaDelete 가 섞여 있을 때
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
		// check if this transaction is used.
		if (!used.compareAndSet(false, true)){
			throw new IllegalStateException("this transaction is already used.");
		}
	}
	
	/**
	 * Determine commitMethod among {@link CommitMethod#SINGLE_ROW_READ_ONLY}, {@link CommitMethod#SINGLE_ROW_PUT_ONLY} and
	 * {@link CommitMethod#MULTI_ROW}.
	 * <p> Transaction of single row with only {@link HaeinsaPut} will be considered as {@link CommitMethod#SINGLE_ROW_PUT_ONLY},
	 * otherwise considered as {@link CommitMethod#MULTI_ROW}.
	 * @return
	 */
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
	
	/**
	 * 
	 * @throws IOException ConflictException, HBase IOException
	 */
	protected void commitMultiRows() throws IOException{		
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// prewrite primary row
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			try{
				table.prewrite(primaryRowState, primary.getRow(), true);
			} finally {
				table.close();
			}
		}
		
		// prewrite secondaries
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) 
						&& Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
					//	if this is primaryRow
					continue;
				}
				{
					HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableStateEntry.getKey());
					try{
						table.prewrite(rowStateEntry.getValue(), rowStateEntry.getKey(), false);
					} finally {
						table.close();
					}
				}
			}
		}
		
		makeStable();
	}
	
	/**
	 * Commit single row & PUT only (possibly include get/scan, but not Delete) Transaction.
	 * @throws IOException
	 */
	protected void commitSingleRowPutOnly() throws IOException {
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// commit primary row
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			try{
				table.commitSingleRowPutOnly(primaryRowState, primary.getRow());
			} finally {
				table.close();
			}
		}
	}
	
	/**
	 * Commit single row & read only Transaction.
	 * @throws IOException
	 */
	protected void commitSingleRowReadOnly() throws IOException {
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// commit primary row
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			try{
				table.commitSingleRowReadOnly(primaryRowState, primary.getRow());
			} finally {
				table.close();
			}
		}
	}

	/**
	 * Commit transaction to HBase. It start to prewrite data in HBase and try to change {@link TRowLock}s.
	 * After {@link #commit()} is called, user cannot use this instance again.
	 * @throws IOException ConflictException, HBase IOException.
	 */
	public void commit() throws IOException { 
		// check if this transaction is used.
		if (!used.compareAndSet(false, true)){
			throw new IllegalStateException("this transaction is already used.");
		}
		// determine commitTimestamp & determine primary row
		TRowKey primaryRowKey = null;
		long commitTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		long prewriteTimestamp = ROW_LOCK_MIN_TIMESTAMP;
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				//	TODO primaryRowKey 를 여러 개의 (table, row) 중에서 하나를 random 하게 고르는 방식으로 바꾸는 것이 좋음.
				//	현재와 같이 첫번째 row 를 primaryRowKey 로 설정하는 방식은 Byte array 로 정렬했을 때 가장 앞에 오는 table / region 에 대해서 
				//	hot table / hot region 문제를 일으킬 수 있음
				//
				//	Random 하게 primaryRowKey 를 고를 때 이왕이면 mutations 중에 HaeinsaPut 이 
				//	가장 처음에 있는 Row 를 골라서 applyMutations 에 걸리는 시간을 줄이면 좋겠다.
				if (primaryRowKey == null){
					primaryRowKey = new TRowKey();
					primaryRowKey.setTableName(tableStateEntry.getKey());
					primaryRowKey.setRow(rowStateEntry.getKey());
				}
				HaeinsaRowTransaction rowState = rowStateEntry.getValue();
				commitTimestamp = Math.max(commitTimestamp, 
						Math.max(ROW_LOCK_MIN_TIMESTAMP, rowState.getCurrent().getCommitTimestamp()) + rowState.getIterationCount());
				prewriteTimestamp = Math.max(prewriteTimestamp, 
						Math.max(ROW_LOCK_MIN_TIMESTAMP, rowState.getCurrent().getCommitTimestamp()) + 1);
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
	
	/**
	 * TODO
	 * @throws IOException
	 */
	private void makeStable() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		// commit primary or get more time to commit this.
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			try{
				table.commitPrimary(primaryRowTx, primary.getRow());
			} finally {
				table.close();
			}
		}
		
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				// apply mutations  
				HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableStateEntry.getKey());
				try{
					table.applyMutations(rowStateEntry.getValue(), rowStateEntry.getKey());
					
					if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
						continue;
					}
					// make secondary rows from prewritten to stable
					table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
				} finally {
					table.close();
				}
			}
		}
		
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			try{
				table.makeStable(primaryRowTx, primary.getRow());
			} finally {
				table.close();
			}
		}
	}
	
	protected void recover() throws IOException {
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		if (primaryRowTx.getCurrent().getState() == TRowLockState.PREWRITTEN){
			// prewritten 상태에서는 timeout 보다 primary이 시간이 더 지났으면 abort 시켜야 함.
			if (primaryRowTx.getCurrent().getExpiry() < System.currentTimeMillis()){
				
			}else{
				// expiry 가 지나지 않았다면, recover 를 실패시켜야 함.
				throw new ConflictException();
			}
		}

		switch (primaryRowTx.getCurrent().getState()) {
		case ABORTED:
		case PREWRITTEN: {
			abort();
			break;
		}

		case COMMITTED: {
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
			try{
				table.abortPrimary(primaryRowTx, primary.getRow());
			} finally {
				table.close();
			}
		}
		
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				// delete prewritten  
				HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableStateEntry.getKey());
				try{
					table.deletePrewritten(rowStateEntry.getValue(), rowStateEntry.getKey());
					
					if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) && Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
						continue;
					}
					// make secondary rows from prewritten to stable
					table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
					
				} finally {
					table.close();
				}
			}
		}
		
		{
			HaeinsaTable table = (HaeinsaTable) tablePool.getTable(primary.getTableName());
			try{
				table.makeStable(primaryRowTx, primary.getRow());
			} finally {
				table.close();				
			}
		}

	}
}