package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Haeinsa 에서 하나의 Transaction 을 표현하는 단위이다. 
 * 내부에 하나의 Transaction 을 표현하기 위한 {@link HaeinsaTableTransaction} 을 들고 있으며, 
 * {@link HaeinsaTransactionManager} 로의 reference 를 가지고 있다.
 * 
 * <p>{@link HaeinsaTransactionManager#begin()} 을 통해서 생성되거나
 * {@link HaeinsaTransactionManager#getTransaction()} 을 통해서 생성되어서 사용할 수 있다. 
 * 전자는 새로운 Transaction 을 시작하는 경우에 사용되고, 후자는 실패한 Transaction 을 rollback 하거나 재시도 시킬 때에 사용된다.
 * 
 * <p>하나의 {@link HaeinsaTransaction} 은 {@link #commit()} 이나 {@link #rollback()} 이 되고 나면 더 이상 사용할 수 없다.
 * @author Youngmok Kim
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
		NOTHING,				//	rowTx 가 아무 것도 없을 때 
		SINGLE_ROW_PUT_ONLY,	//	rowTx 가 하나만 존재하고, muation 의 종류가 HaeinsaPut 일 때 
		SINGLE_ROW_READ_ONLY,	//	rowTx 가 하나만 존재하고, muations 가 없을 때 
		MULTI_ROW				//	rowTx 가 여러 개 존재하거나, rowTx 가 하나만 존재하면서 종류가 HaeinsaDelete 이거나 
								//	rowTx 가 하나만 존재하면서 여러 개의 HaeinsaPut / HeainsaDelete 가 섞여 있을 때
	}
	
	public HaeinsaTransaction(HaeinsaTransactionManager manager) {
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
	
	/**
	 * tableName 을 가지는 {@link HaeinsaTableTransaction} 을 가져온다. 
	 * 만약 해당 이름의 {@link HaeinsaTableTrasaction} 이 존재하지 않으면 새로 instance 를 생성해서 
	 * HaeinsaTransaction 내부의 {@link #tableStates} 에 저장하고 return 한다.
	 * @param tableName
	 * @return
	 */
	protected HaeinsaTableTransaction createOrGetTableState(byte[] tableName) {
		HaeinsaTableTransaction tableTxState = tableStates.get(tableName);
		if (tableTxState == null) {
			tableTxState = new HaeinsaTableTransaction(this);
			tableStates.put(tableName, tableTxState);
		}
		return tableTxState;
	}
	
	public void rollback() throws IOException {
		// check if this transaction is used.
		if (!used.compareAndSet(false, true)) {
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
	protected CommitMethod determineCommitMethod() {
		int count = 0;
		CommitMethod method = CommitMethod.NOTHING;
		for (HaeinsaTableTransaction tableState : getTableStates().values()) {
			for (HaeinsaRowTransaction rowState : tableState.getRowStates().values()) {
				count++;
				if (count > 1) {
					return CommitMethod.MULTI_ROW;
				}
				if (rowState.getMutations().size() <= 0) {
					method = CommitMethod.SINGLE_ROW_READ_ONLY;
				} else if (rowState.getMutations().get(0) instanceof HaeinsaPut && rowState.getMutations().size() == 1) {
					method = CommitMethod.SINGLE_ROW_PUT_ONLY;
				} else {
					method = CommitMethod.MULTI_ROW;
				}
			}
		}
		return method;
	}
	
	/**
	 * Commit multiple row Transaction or single row Transaction which includes Delete operation.
	 * 
	 * @throws IOException ConflictException, HBase IOException
	 */
	protected void commitMultiRows() throws IOException {		
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		
		HaeinsaTablePool tablePool = getManager().getTablePool();
		// prewrite primary row
		{
			HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName());
			try {
				table.prewrite(primaryRowState, primary.getRow(), true);
			} finally {
				table.close();
			}
		}
		
		// prewrite secondaries
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
				if (Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) 
						&& Bytes.equals(rowStateEntry.getKey(), primary.getRow())) {
					//	if this is primaryRow
					continue;
				}
				{
					HaeinsaTableIfaceInternal table = tablePool.getTableInternal(tableStateEntry.getKey());
					try {
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
			HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName());
			try {
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
			HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName());
			try {
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
		if (!used.compareAndSet(false, true)) {
			throw new IllegalStateException("this transaction is already used.");
		}
		// determine commitTimestamp & determine primary row
		TRowKey primaryRowKey = null;
		long maxCurrentCommitTimestamp = System.currentTimeMillis();
		long maxIterationCount = Long.MIN_VALUE;
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
				//	TODO primaryRowKey 를 여러 개의 (table, row) 중에서 하나를 random 하게 고르는 방식으로 바꾸는 것이 좋음.
				//	현재와 같이 첫번째 row 를 primaryRowKey 로 설정하는 방식은 Byte array 로 정렬했을 때 가장 앞에 오는 table / region 에 대해서 
				//	hot table / hot region 문제를 일으킬 수 있음
				//
				//	Random 하게 primaryRowKey 를 고를 때 이왕이면 mutations 중에 HaeinsaPut 이 
				//	가장 처음에 있는 Row 를 골라서 applyMutations 에 걸리는 시간을 줄이면 좋겠다.
				if (primaryRowKey == null) {
					primaryRowKey = new TRowKey();
					primaryRowKey.setTableName(tableStateEntry.getKey());
					primaryRowKey.setRow(rowStateEntry.getKey());
				}
				HaeinsaRowTransaction rowState = rowStateEntry.getValue();
				maxIterationCount = Math.max(maxIterationCount, rowState.getIterationCount());
				maxCurrentCommitTimestamp = Math.max(maxCurrentCommitTimestamp, rowState.getCurrent().getCommitTimestamp());				
			}
		}
		setPrimary(primaryRowKey);
		
		setPrewriteTimestamp(maxCurrentCommitTimestamp + 1);
		setCommitTimestamp(Math.max(getPrewriteTimestamp(), maxCurrentCommitTimestamp + maxIterationCount));
		
		
		CommitMethod method = determineCommitMethod();
		switch (method) {
		case MULTI_ROW: {
			commitMultiRows();
			break;
		}
		case SINGLE_ROW_READ_ONLY: {
			commitSingleRowReadOnly();
			break;
		}
		case SINGLE_ROW_PUT_ONLY: {
			commitSingleRowPutOnly();
			break;
		}
		case NOTHING: {
			break;
		}
		default:
			break;
		}
		
	}
	
	/**
	 * 하나의 Transaction 에 해당하는 모든 row 의 {@link TRowLock} 의 state 를 {@link TRowLockState#STABLE} 로 바꾼다.
	 * 다음 2가지 경우에 불릴 수 있다.  
	 * <p> 1. {@link #commitMultiRows()} 에서 primary row 를 {@link TRowLockState#COMMITTED} 로 바꾸고 
	 * primary row 와 secondary row 의 mutation 을 모두 적용한 후에 {@link TRowLockState#STABLE} 로 바꾸는 작업을 수행한다. 
	 * <p> 2. {@link #recover()} 에서부터 불려서 중간에 실패한 Transaction 을 완성시키는 작업을 수행한다. 
	 * 이 함수가 실행되기 위해선 primary row 가 {@link TRowLockState#COMMITTED} 에 와 있어야 한다.
	 *  
	 * @throws IOException ConflictException, HBase IOException.
	 */
	private void makeStable() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		// commit primary or get more time to commit this.
		{
			HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName());
			try {
				//	
				//	commit 이 2번 일어날 수 있는데, 복원하는 Client 가 lock 에 대한 권한을 가질려면 expiry 를 추가로 늘려야 하기 때문입니다.
				table.commitPrimary(primaryRowTx, primary.getRow());
			} finally {
				table.close();
			}
		}
		//	이 지점에 도달하면 이 transaction 은 이미 성공한 것으로 취급됩니다. 
		
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
				// apply mutations  
				HaeinsaTableIfaceInternal table = tablePool.getTableInternal(tableStateEntry.getKey());
				try {
					table.applyMutations(rowStateEntry.getValue(), rowStateEntry.getKey());
					
					if (Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) 
							&& Bytes.equals(rowStateEntry.getKey(), primary.getRow())) {
						//	primary row 일 때 
						continue;
					}
					// make secondary rows from prewritten to stable
					table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
				} finally {
					table.close();
				}
			}
		}
		
		//	make primary row stable
		{
			HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName());
			try {
				table.makeStable(primaryRowTx, primary.getRow());
			} finally {
				table.close();
			}
		}
	}
	
	/**
	 * 과거에 시도되었지만 완료되지 못한 Transaction 을 재현한 후에 이미 성공한 Transaction 이면 ( primaryRow 가 {@link TRowLockState#COMMITTED} 이면 )
	 * {@link #makeStable()} method 를 불러서 stable 시키고,
	 * 아직 commit 되지 못한 Transaction 일 경우엔 {@link #abort()} method 를 부른다.
	 * 
	 * @throws IOException
	 */
	protected void recover() throws IOException {
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		if (primaryRowTx.getCurrent().getState() == TRowLockState.PREWRITTEN) {
			// prewritten 상태에서는 timeout 보다 primary이 시간이 더 지났으면 abort 시켜야 함.
			if (primaryRowTx.getCurrent().getExpiry() < System.currentTimeMillis()) {
				
			} else {
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
			//	이미 성공한 Transaction 이다. 
			makeStable();
			break;
		}
		default:
			throw new ConflictException();
		}
	}

	/**
	 * Transaction 을 abort 시켜서 Transaction 을 시작하기 전의 상태로 되돌리는 method 이다.
	 * Transaction 을 진행하던 Client 가 lock 을 가져오지 못하는 등의 이유로 취소 시킬 수 있고, 
	 * 다른 Client 가 시도하던 Transaction 이 실패하고 expiry 가 지난 후에 취소 시킬 수도 있다. 
	 * abort 는 기본적으로 lazy-recovery 로 진행된다.    
	 * <p> 다른 Client 가 시도한 Transaction 을 rollback 하는 작업을 진행하는 경우에는 
	 * 실패한 Transaction 의 상태를 primary row 의 lock 에 담긴 secondary 정보와 
	 * secondary row 들의 lock 에 담긴 정보를 통해서 복구되었다고 가정한다.  
	 * <p> 다음과 같은 순서로 abort 가 진행된다. 
	 * <p> 1. primary row 를 abort 시킨다. ( {@link HaeinsaTableIfaceInternal#abortPrimary()} )
	 * <p> 2. secondary row 들을 돌아가면서 prewritten 을 지우고 stable 로 바꾼다.  
	 * <p> 3. primary row 를 stable 로 바꾼다. 
	 *  
	 * @throws IOException ConflictException, HBase IOException.
	 */
	protected void abort() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		{
			// abort primary row
			HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName());
			try {
				table.abortPrimary(primaryRowTx, primary.getRow());
			} finally {
				table.close();
			}
		}
		
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
				// delete prewritten, transaction 에 포함된 row 마다 table 이 다를 수 있기 때문에 HaeinsaTable 을 매번 다시 받아야 한다.
				HaeinsaTableIfaceInternal table = tablePool.getTableInternal(tableStateEntry.getKey());
				try {
					table.deletePrewritten(rowStateEntry.getValue(), rowStateEntry.getKey());
					
					if (Bytes.equals(tableStateEntry.getKey(), primary.getTableName()) 
							&& Bytes.equals(rowStateEntry.getKey(), primary.getRow())) {
						continue;
					}
					// make secondary rows from prewritten to stable
					table.makeStable(rowStateEntry.getValue(), rowStateEntry.getKey());
					
				} finally {
					table.close();
				}
			}
		}

		//	make primary row stable
		{
			HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName());
			try {
				table.makeStable(primaryRowTx, primary.getRow());
			} finally {
				table.close();				
			}
		}
	}
}
