package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

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
 * @author Youngmok Kim, Myungbo Kim
 *
 */
public class HaeinsaTransaction {
	private final HaeinsaTransactionState txStates = new HaeinsaTransactionState();

	private final HaeinsaTransactionManager manager;
	private TRowKey primary;
	private long commitTimestamp = Long.MIN_VALUE;
	private long prewriteTimestamp = Long.MIN_VALUE;
	private final AtomicBoolean used = new AtomicBoolean(false);

	//	CommitMethod에서 SingleRowReadOnly method를 MultiRowReadOnly 와 합칠 수 있을 것 같다.
	private static enum CommitMethod {
		NOTHING,				//	rowTx 가 아무 것도 없을 때
		SINGLE_ROW_PUT_ONLY,	//	rowTx 가 하나만 존재하고, mutation 의 종류가 HaeinsaPut 일 때
		SINGLE_ROW_READ_ONLY,	//	rowTx 가 하나만 존재하고, mutation 가 없을 때
		MULTI_ROW_READ_ONLY,	//	rowTx 가 여러 개 존재하고, 모든 rowTx 가 Get/Scan 으로 이루어져 있을 때
		MULTI_ROW_MUTATIONS,	//	rowTx 가 여러 개 존재하고 최소 1개의 rowTx 에 mutation 이 존재하거나,
								//	rowTx 가 하나만 존재하면서 mutation 에 HaeinsaDelete 가 포함되어 있을 때

	}

	public HaeinsaTransaction(HaeinsaTransactionManager manager){
		this.manager = manager;
	}

	protected NavigableMap<TRowKey, HaeinsaRowTransaction> getMutationRowStates(){
		return txStates.getMutationRowStates();
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
	protected HaeinsaTableTransaction createOrGetTableState(byte[] tableName){
		HaeinsaTableTransaction tableTxState = txStates.getTableStates().get(tableName);
		if (tableTxState == null){
			tableTxState = new HaeinsaTableTransaction(this);
			txStates.getTableStates().put(tableName, tableTxState);
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
	 * Determine commitMethod among {@link CommitMethod#SINGLE_ROW_READ_ONLY}, {@link CommitMethod#SINGLE_ROW_PUT_ONLY},
	 * {@link CommitMethod#MULTI_ROW_READ_ONLY} and {@link CommitMethod#MULTI_ROW_MUTATIONS}.
	 * <p> Transaction of single row with at least one of {@link HaeinsaDelete} will be considered as
	 * {@link CommitMethod#MULTI_ROW_MUTATIONS}.
	 * @return
	 */
	protected CommitMethod determineCommitMethod(){
		int count = 0;
		boolean haveMuations = false;
		CommitMethod method = CommitMethod.NOTHING;
		for (HaeinsaTableTransaction tableState : txStates.getTableStates().values()){
			for (HaeinsaRowTransaction rowState : tableState.getRowStates().values()){
				count ++;
				if(rowState.getMutations().size()>0){
					//	if any rowTx in Tx contains mutation ( Put/Delete )
					haveMuations = true;
				}

				if (count==1){
					if (rowState.getMutations().size() <= 0){
						method = CommitMethod.SINGLE_ROW_READ_ONLY;
					}else if (rowState.getMutations().get(0) instanceof HaeinsaPut && rowState.getMutations().size() == 1) {
						method = CommitMethod.SINGLE_ROW_PUT_ONLY;
					}else if(haveMuations){
						method = CommitMethod.MULTI_ROW_MUTATIONS;	//	if rowTx contains HaeinsaDelete
					}
				}
				if (count > 1){
					if(haveMuations)
						return CommitMethod.MULTI_ROW_MUTATIONS;
					else
						method = CommitMethod.MULTI_ROW_READ_ONLY;
				}
			}
		}
		return method;
	}

	/**
	 * Only-read multiRow don't have to do anything if conflict found.
	 * Commit multiple row Transaction or single row Transaction which includes Delete operation.
	 *
	 * @throws IOException ConflictException, HBase IOException
	 */
	protected void commitMultiRowsMutation() throws IOException{
		Preconditions.checkState(txStates.getMutationRowStates().size()>0);
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());

		HaeinsaTablePool tablePool = getManager().getTablePool();
		// prewrite primary row (mutation row)
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.prewrite(primaryRowState, primary.getRow(), true);
		}

		// prewrite secondaries (mutation rows)
		for(Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getMutationRowStates().entrySet()){
			TRowKey rowKey = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			if ((Bytes.equals(rowKey.getTableName(), primary.getTableName())
					&& Bytes.equals(rowKey.getRow(), primary.getRow()))){
				//	if this is primaryRow
				continue;
			}
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(rowKey.getTableName())) {
				table.prewrite(rowTx, rowKey.getRow(), false);
			}
		}

		//	check locking of secondaries by get (read-only rows)
		for(Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getReadOnlyRowStates().entrySet()){
			TRowKey rowKey = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(rowKey.getTableName())) {
				table.checkSingleRowLock(rowTx, rowKey.getRow());
			}
		}
		makeStable();
	}


	/**
	 * Use {@link HaeinsaTable#commitSingleRowReadOnly()} to check RowLock on HBase of read-only row of tx.
	 * If all lock-checking by get was success, read-only multi-row tx was success.
	 * Throws Exception otherwise.
	 *
	 * @throws IOException ConflictException, HBase IOException
	 */
	protected void commitMultiRowsReadOnly() throws IOException{
		Preconditions.checkState(txStates.getMutationRowStates().size()==0);
		Preconditions.checkState(txStates.getReadOnlyRowStates().size()>0);
		HaeinsaTablePool tablePool = getManager().getTablePool();

		// check secondaries
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : txStates.getTableStates().entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				if ((Bytes.equals(tableStateEntry.getKey(), primary.getTableName())
						&& Bytes.equals(rowStateEntry.getKey(), primary.getRow()))){
					//	if this is primaryRow
					continue;
				}
				try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(tableStateEntry.getKey())) {
					table.checkSingleRowLock(rowStateEntry.getValue(), rowStateEntry.getKey());
				}
			}
		}

		//	check primary last
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.checkSingleRowLock(primaryRowState, primary.getRow());
		}
		//	do not need stable-phase
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
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.commitSingleRowPutOnly(primaryRowState, primary.getRow());
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
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.commitSingleRowReadOnly(primaryRowState, primary.getRow());
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
		long maxCurrentCommitTimestamp = System.currentTimeMillis();
		long maxIterationCount = Long.MIN_VALUE;

		// 	determine commitTimestamp & determine primary row
		//	fill mutationRowStates & readOnlyRowStates from rowStates
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : txStates.getTableStates().entrySet()){
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
				//	TODO(Andrew): Random 하게 primaryRowKey 를 고를 때 이왕이면 mutations 중에 HaeinsaPut 이
				//	가장 처음에 있는 Row 를 골라서 applyMutations 에 걸리는 시간을 줄이면 좋겠다.

				HaeinsaRowTransaction rowState = rowStateEntry.getValue();
				maxIterationCount = Math.max(maxIterationCount, rowState.getIterationCount());
				maxCurrentCommitTimestamp = Math.max(maxCurrentCommitTimestamp, rowState.getCurrent().getCommitTimestamp());
			}
		}
		setPrewriteTimestamp(maxCurrentCommitTimestamp + 1);
		setCommitTimestamp(Math.max(getPrewriteTimestamp(), maxCurrentCommitTimestamp + maxIterationCount));

		//	setPrimary among mutationRowStates first, next among readOnlyRowStates
		TRowKey primaryRowKey = null;
		NavigableMap<TRowKey, HaeinsaRowTransaction> mutationRowStates = txStates.getMutationRowStates();
		NavigableMap<TRowKey, HaeinsaRowTransaction> readOnlyRowStates = txStates.getReadOnlyRowStates();
		if(mutationRowStates.size()>0){
			//	if there is any mutation row, choose first one among muation row.
			primaryRowKey = mutationRowStates.firstKey();
		}
		else if(readOnlyRowStates.size()>0){
			//	if there is no mutation row at all, choose first one among read-only row.
			primaryRowKey = readOnlyRowStates.firstKey();
		}
		//	primaryRowKey can be null at this point, which means there is no rowStates at all.
		setPrimary(primaryRowKey);

		CommitMethod method = determineCommitMethod();
		switch (method) {
		case MULTI_ROW_MUTATIONS:{
			commitMultiRowsMutation();
			break;
		}
		case MULTI_ROW_READ_ONLY:{
			commitMultiRowsReadOnly();
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

		case NOTHING:{
			break;
		}

		default:
			break;
		}

	}

	/**
	 * 하나의 Transaction 에 해당하는 모든 mutation row 의 {@link TRowLock} 의 state 를 {@link TRowLockState#STABLE} 로 바꾼다.
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
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			//	commit 이 2번 일어날 수 있는데, 복원하는 Client 가 lock 에 대한 권한을 가질려면 expiry 를 추가로 늘려야 하기 때문입니다.
			table.commitPrimary(primaryRowTx, primary.getRow());
		}
		//	이 지점에 도달하면 이 transaction 은 이미 성공한 것으로 취급됩니다.

		//	Change state of secondary rows to stable
		for(Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry: txStates.getMutationRowStates().entrySet()){
			TRowKey rowKey = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(rowKey.getTableName())) {
				table.applyMutations(rowTx, rowKey.getRow());
				if ((Bytes.equals(rowKey.getTableName(), primary.getTableName())
						&& Bytes.equals(rowKey.getRow(), primary.getRow()))){
					//	primary row 일 때
					continue;

				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowTx, rowKey.getRow());
			}
		}

		//	make primary row stable
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.makeStable(primaryRowTx, primary.getRow());
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
		if (primaryRowTx.getCurrent().getState() == TRowLockState.PREWRITTEN){
			// prewritten 상태에서는 timeout 보다 primary이 시간이 더 지났으면 abort 시켜야 함.
			if (primaryRowTx.getCurrent().getExpiry() < System.currentTimeMillis()){

			}else{
				// if transaction haven't past expiry, recover should be failed.
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
			//	Transaction is already succeeded.
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
		// abort primary row
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.abortPrimary(primaryRowTx, primary.getRow());
		}

		//	recover secondary mutation rows
		for(Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getMutationRowStates().entrySet()){
			TRowKey rowKey = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(rowKey.getTableName())) {
				table.deletePrewritten(rowTx, rowKey.getRow());

				if ((Bytes.equals(rowKey.getTableName(), primary.getTableName())
						&& Bytes.equals(rowKey.getRow(), primary.getRow()))){
					continue;

				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowTx, rowKey.getRow());

			}
		}

		//	make primary row stable
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.makeStable(primaryRowTx, primary.getRow());
		}

	}

	/**
	 * Container which contain {byte[] : {@link HaeinsaTableTransaction}} map.
	 * <p>This class is not Thread-safe.
	 * This class will separate each {@link HaeinsaRowTransaction} to ReadOnlyRowStates and MutationRowStates.
	 * <p>If rowState have more than 1 mutations or state of row is not {@link TRowLockState#STABLE},
	 * then that row is MutationRow. ReadOnlyRow otherwise (There is no mutations, and state is STABLE).
	 * @author Myungbo Kim
	 *
	 */
	private static class HaeinsaTransactionState{
		private final NavigableMap<byte[], HaeinsaTableTransaction> tableStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
		private final Comparator<TRowKey> comparator = new HashComparator();

		public NavigableMap<byte[], HaeinsaTableTransaction> getTableStates(){
			return tableStates;
		}

		public NavigableMap<TRowKey, HaeinsaRowTransaction> getMutationRowStates(){
			TreeMap<TRowKey, HaeinsaRowTransaction> map = Maps.newTreeMap(comparator);
			for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
				for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
					HaeinsaRowTransaction rowState = rowStateEntry.getValue();
					TRowKey rowKey = new TRowKey();
					rowKey.setTableName(tableStateEntry.getKey());
					rowKey.setRow(rowStateEntry.getKey());
					if(rowState.getMutations().size()>0 || rowState.getCurrent().getState()!=TRowLockState.STABLE){
						map.put(rowKey, rowState);
					}
				}
			}
			return map;
		}

		public NavigableMap<TRowKey, HaeinsaRowTransaction> getReadOnlyRowStates(){
			TreeMap<TRowKey, HaeinsaRowTransaction> map = Maps.newTreeMap(comparator);
			for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
				for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
					HaeinsaRowTransaction rowState = rowStateEntry.getValue();
					TRowKey rowKey = new TRowKey();
					rowKey.setTableName(tableStateEntry.getKey());
					rowKey.setRow(rowStateEntry.getKey());
					if(rowState.getMutations().size()<=0 && rowState.getCurrent().getState()!=TRowLockState.STABLE){
						map.put(rowKey, rowState);
					}
				}
			}
			return map;
		}
	}

	/**
	 * Basic comparator which use lexicographical ordering of (byte[] table, byte[] row).
	 * Thread-safe & stateless
	 * @author Myungbo Kim
	 *
	 */
	private static class BasicComparator implements Comparator<TRowKey>{

		@Override
		public int compare(TRowKey o1, TRowKey o2) {
			return ComparisonChain.start()
					.compare(o1.getTableName(), o2.getTableName(), Bytes.BYTES_COMPARATOR)
					.compare(o1.getRow(), o2.getRow(), Bytes.BYTES_COMPARATOR)
					.result();
		}

	}

	/**
	 * Comparator which will deterministically order processing of each row.
	 * <p> Get guava murmur3_32bit hash value of (byte[] table, byte[] row), and compare those two to order {@link TRowKey}
	 * @author Myungbo Kim
	 *
	 */
	private static class HashComparator implements Comparator<TRowKey>{
		private static HashFunction HASH = Hashing.murmur3_32();
		private static Comparator<TRowKey> BASIC_COMP = new BasicComparator();

		@Override
		public int compare(TRowKey o1, TRowKey o2) {

			int hash1 = HASH.newHasher().putBytes(o1.getTableName()).putBytes(o1.getRow()).hash().asInt();
			int hash2 = HASH.newHasher().putBytes(o1.getTableName()).putBytes(o1.getRow()).hash().asInt();
			if(hash1>hash2)
				return 1;
			else if(hash1==hash2){
				return BASIC_COMP.compare(o1, o2);
			}
			else
				return -1;
		}

	}
}
