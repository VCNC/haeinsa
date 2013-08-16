/**
 * Copyright (C) 2013 VCNC, inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.exception.RecoverableConflictException;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of single transaction in Haeinsa.
 * It contains {@link HaeinsaTableTransaction}s to include information of overall transaction,
 * and have reference to {@link HaeinsaTransactionManager} which created this instance.
 * <p>
 * HaeinsaTransaction can be generated via calling {@link HaeinsaTransactionManager#begin()}
 * or {@link HaeinsaTransactionManager#getTransaction()}.
 * Former is used when start new transaction, later is used when try to roll back or retry failed transaction.
 * <p>
 * One {@link HaeinsaTransaction} can't be used after calling {@link #commit()} or {@link #rollback()} is called.
 */
public class HaeinsaTransaction {
	private static final Logger LOGGER = LoggerFactory.getLogger(HaeinsaTransaction.class);
	private final HaeinsaTransactionState txStates = new HaeinsaTransactionState();

	private final HaeinsaTransactionManager manager;
	private TRowKey primary;
	private long commitTimestamp = Long.MIN_VALUE;
	private long prewriteTimestamp = Long.MIN_VALUE;
	private final AtomicBoolean used = new AtomicBoolean(false);

	private static enum CommitMethod {
		/**
		 * If all rowTx do not have mutation. (only consisted with Get/Scan)
		 */
		READ_ONLY,
		/**
		 * If there is only one rowTx and type of its mutation is HaeinsaPut.
		 */
		SINGLE_ROW_PUT_ONLY,
		/**
		 * When there is multiple rowTx and at least one of that include mutation,
		 * or there is only one rowTx and its mutation contains HaeinsaDelete.
		 */
		MULTI_ROW_MUTATIONS,
		/**
		 * If there is no rowTx (there is no actual DB access).
		 */
		NOTHING;
	}

	public HaeinsaTransaction(HaeinsaTransactionManager manager) {
		this.manager = manager;
	}

	protected NavigableMap<TRowKey, HaeinsaRowTransaction> getMutationRowStates() {
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
	 * Bring {@link HaeinsaTableTransaction} which have name of tableName.
	 * If there is no {@link HaeinsaTableTransaction} have this name,
	 * then create one instance for it and save inside {@link #tableStates} and return.
	 *
	 * @param tableName
	 * @return
	 */
	protected HaeinsaTableTransaction createOrGetTableState(byte[] tableName) {
		HaeinsaTableTransaction tableTxState = txStates.getTableStates().get(tableName);
		if (tableTxState == null) {
			tableTxState = new HaeinsaTableTransaction(this);
			txStates.getTableStates().put(tableName, tableTxState);
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
	 * Commit transaction to HBase. It start to prewrite data in HBase and try
	 * to change {@link TRowLock}s. After {@link #commit()} is called, user
	 * cannot use this instance again.
	 *
	 * @throws IOException ConflictException, HBase IOException.
	 */
	public void commit() throws IOException {
		// check if this transaction is used.
		if (!used.compareAndSet(false, true)) {
			throw new IllegalStateException("this transaction is already used.");
		}
		boolean onRecovery = false;
		txStates.classifyAndSortRows(onRecovery);
		long maxCurrentCommitTimestamp = System.currentTimeMillis();
		long maxIterationCount = Long.MIN_VALUE;

		// determine commitTimestamp & determine primary row
		// fill mutationRowStates & readOnlyRowStates from rowStates
		for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : txStates.getTableStates().entrySet()) {
			for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
				HaeinsaRowTransaction rowState = rowStateEntry.getValue();
				maxIterationCount = Math.max(maxIterationCount, rowState.getIterationCount());
				maxCurrentCommitTimestamp = Math.max(maxCurrentCommitTimestamp, rowState.getCurrent().getCommitTimestamp());
			}
		}

		// HBase compaction시에 같은 timestamp에 값을 쓰면 compaction후에 최신 값이 남이 있는 것이 보장되지 않는다.
		setPrewriteTimestamp(maxCurrentCommitTimestamp + 1);
		setCommitTimestamp(Math.max(getPrewriteTimestamp() + 2, maxCurrentCommitTimestamp + maxIterationCount + 2));

		// setPrimary among mutationRowStates first, next among
		// readOnlyRowStates
		TRowKey primaryRowKey = null;
		NavigableMap<TRowKey, HaeinsaRowTransaction> mutationRowStates = txStates.getMutationRowStates();
		NavigableMap<TRowKey, HaeinsaRowTransaction> readOnlyRowStates = txStates.getReadOnlyRowStates();
		if (mutationRowStates.size() > 0) {
			// if there is any mutation row, choose first one among muation row.
			primaryRowKey = mutationRowStates.firstKey();
		} else if (readOnlyRowStates.size() > 0) {
			// if there is no mutation row at all, choose first one among
			// read-only row.
			primaryRowKey = readOnlyRowStates.firstKey();
		}
		// primaryRowKey can be null at this point, which means there is no
		// rowStates at all.
		setPrimary(primaryRowKey);

		CommitMethod method = txStates.determineCommitMethod();
		switch (method) {
		case READ_ONLY: {
			commitReadOnly();
			break;
		}
		case SINGLE_ROW_PUT_ONLY: {
			commitSingleRowPutOnly();
			break;
		}
		case MULTI_ROW_MUTATIONS: {
			commitMultiRowsMutation();
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
	 * Use {@link HaeinsaTable#checkSingleRowLock()} to check RowLock on HBase
	 * of read-only rows of tx. If all lock-checking by get was success,
	 * read-only tx was success. Throws ConflictException otherwise.
	 *
	 * @throws IOException ConflictException, HBase IOException
	 */
	private void commitReadOnly() throws IOException {
		Preconditions.checkState(txStates.getMutationRowStates().size() == 0);
		Preconditions.checkState(txStates.getReadOnlyRowStates().size() > 0);
		HaeinsaTablePool tablePool = getManager().getTablePool();

		// check secondaries
		for (Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getReadOnlyRowStates().entrySet()) {
			TRowKey key = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			if (Bytes.equals(key.getTableName(), primary.getTableName())
					&& Bytes.equals(key.getRow(), primary.getRow())) {
				// if this is primaryRow
				continue;
			}
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(key.getTableName())) {
				table.checkSingleRowLock(rowTx, key.getRow());
			}
		}

		// check primary last
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.checkSingleRowLock(primaryRowState, primary.getRow());
		}
		// do not need stable-phase
	}

	/**
	 * Commit single row & PUT only (possibly include get/scan, but not Delete)
	 * Transaction.
	 *
	 * @throws IOException
	 */
	private void commitSingleRowPutOnly() throws IOException {
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());

		HaeinsaTablePool tablePool = getManager().getTablePool();
		// commit primary row
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.commitSingleRowPutOnly(primaryRowState, primary.getRow());
		}
	}

	/**
	 * Commit multiple row Transaction or single row Transaction which includes
	 * Delete operation.
	 *
	 * @throws IOException ConflictException, HBase IOException
	 */
	private void commitMultiRowsMutation() throws IOException {
		Preconditions.checkState(txStates.getMutationRowStates().size() > 0);
		HaeinsaTableTransaction primaryTableState = createOrGetTableState(primary.getTableName());
		HaeinsaRowTransaction primaryRowState = primaryTableState.createOrGetRowState(primary.getRow());

		HaeinsaTablePool tablePool = getManager().getTablePool();
		// prewrite primary row (mutation row)
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.prewrite(primaryRowState, primary.getRow(), true);
		}

		// prewrite secondaries (mutation rows)
		for (Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getMutationRowStates().entrySet()) {
			TRowKey key = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			if (Bytes.equals(key.getTableName(), primary.getTableName())
					&& Bytes.equals(key.getRow(), primary.getRow())) {
				// if this is primaryRow
				continue;
			}
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(key.getTableName())) {
				table.prewrite(rowTx, key.getRow(), false);
			}
		}

		// check locking of secondaries by get (read-only rows)
		for (Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getReadOnlyRowStates().entrySet()) {
			TRowKey rowKey = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(rowKey.getTableName())) {
				table.checkSingleRowLock(rowTx, rowKey.getRow());
			}
		}

		makeStable();
	}

	/**
	 * Change states of {@link TRowLock} of all mutation rows to {@link TRowLockState#STABLE}.
	 * This can be called by following two cases.
	 * <p>
	 * 1. In case of {@link #commitMultiRowsMutation()}, after changing primary row to
	 * {@link TRowLockState#COMMITTED} and applying all mutations in primary row and secondary rows.
	 * <p>
	 * 2. When try to {@link #recover()} failed transaction in the middle of execution.
	 * This method should be called only when primary row is in the state of {@link TRowLockState#COMMITTED}.
	 *
	 * @throws IOException ConflictException, HBase IOException.
	 */
	private void makeStable() throws IOException {
		HaeinsaTablePool tablePool = getManager().getTablePool();
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName())
				.createOrGetRowState(primary.getRow());
		// commit primary or get more time to commit this.
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			// commitPrimary can be happened two times, this is because recovering client need to
			// extend expiry during recovering.
			table.commitPrimary(primaryRowTx, primary.getRow());
		}
		//	if transaction reached this state, the transaction is considered as success one.
		try {
			// Change state of secondary rows to stable
			for (Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getMutationRowStates().entrySet()) {
				TRowKey rowKey = rowKeyStateEntry.getKey();
				HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
				try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(rowKey.getTableName())) {
					table.applyMutations(rowTx, rowKey.getRow());
					if (Bytes.equals(rowKey.getTableName(), primary.getTableName())
							&& Bytes.equals(rowKey.getRow(), primary.getRow())) {
						//	in case of primary row
						continue;
					}
					// make secondary rows from prewritten to stable
					table.makeStable(rowTx, rowKey.getRow());
				}
			}

			// make primary row stable
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
				table.makeStable(primaryRowTx, primary.getRow());
			}
		} catch (RecoverableConflictException e) {
			// if making row stable is failed, but primary is committed. Then treat this transaction as succeeded.
			LOGGER.warn(e.getMessage(), e);
		}
	}

	/**
	 * Reload information of failed transaction and complete it by calling {@link #makStable()}
	 * if already completed one, ( when primaryRow have {@link TRowLockState#COMMITTED} state }
	 * or abort by calling {@link #abort()} otherwise.
	 *
	 * @throws IOException
	 */
	protected void recover() throws IOException {
		boolean onRecovery = true;
		txStates.classifyAndSortRows(onRecovery);
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		if (primaryRowTx.getCurrent().getState() == TRowLockState.PREWRITTEN) {
			// If primary row is in prewritten state, transaction can be aborted only after expiry.
			if (primaryRowTx.getCurrent().getExpiry() < System.currentTimeMillis()) {
				// if transaction is not expired, process recover
			} else {
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
			// Transaction is already succeeded.
			makeStable();
			break;
		}
		default:
			throw new ConflictException();
		}
	}

	/**
	 * Method that abort transaction and make rows to state before transaction was started.
	 * Transaction can be canceled by client which started it when failed to acquire lock of mutation row,
	 * or by other client which try to access any row of failed transaction which have past expiry.
	 * Aborting failed transaction is basically processed by lazy-recovering.
	 * <p>
	 * When try to roll back failed transaction started by other client,
	 * this method assume that state of failed transaction is properly loaded from
	 * locks of primary and secondary rows to {@link #txStates} of this instance.
	 * <p>
	 * Aborting is executed by following order.
	 * <ol>
	 * <li>Abort primary row by calling {@link HaeinsaTableIfaceInternal#abortPrimary()}.</li>
	 * <li>Visit all secondary rows and change from prewritten to stable state.
	 * Prewritten data on rows are removed at this state.</li>
	 * <li>Change primary row to stable state.</li>
	 * </ol>
	 *
	 * @throws IOException ConflictException, HBase IOException.
	 */
	protected void abort() throws IOException {
		if (txStates.getMutationRowStates().size() == 0) {
			// if commitReadOnly fails, don't abort
			return;
		}

		HaeinsaTablePool tablePool = getManager().getTablePool();
		HaeinsaRowTransaction primaryRowTx = createOrGetTableState(primary.getTableName()).createOrGetRowState(primary.getRow());
		// abort primary row
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.abortPrimary(primaryRowTx, primary.getRow());
		}

		// recover secondary mutation rows
		for (Entry<TRowKey, HaeinsaRowTransaction> rowKeyStateEntry : txStates.getMutationRowStates().entrySet()) {
			TRowKey rowKey = rowKeyStateEntry.getKey();
			HaeinsaRowTransaction rowTx = rowKeyStateEntry.getValue();
			if (rowTx.getCurrent().getState() == TRowLockState.STABLE) {
				// abort only unstable rows
				continue;
			}
			try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(rowKey.getTableName())) {
				table.deletePrewritten(rowTx, rowKey.getRow());

				if (Bytes.equals(rowKey.getTableName(), primary.getTableName())
						&& Bytes.equals(rowKey.getRow(), primary.getRow())) {
					continue;
				}
				// make secondary rows from prewritten to stable
				table.makeStable(rowTx, rowKey.getRow());
			}
		}

		// make primary row stable
		try (HaeinsaTableIfaceInternal table = tablePool.getTableInternal(primary.getTableName())) {
			table.makeStable(primaryRowTx, primary.getRow());
		}
	}

	/**
	 * for unit test code
	 * @param onRecovery onRecovery
	 */
	void classifyAndSortRows(boolean onRecovery) {
		txStates.classifyAndSortRows(onRecovery);
	}

	/**
	 * Container which contain {byte[] : {@link HaeinsaTableTransaction} map.
	 * <p>
	 * This class is not Thread-safe. This class will separate each
	 * {@link HaeinsaRowTransaction} to ReadOnlyRowStates and MutationRowStates.
	 * <p>
	 * If rowState have more than 1 mutations or state of row is not
	 * {@link TRowLockState#STABLE}, then that row is MutationRow. ReadOnlyRow
	 * otherwise (There is no mutations, and state is STABLE).
	 */
	private static class HaeinsaTransactionState {
		private final NavigableMap<byte[], HaeinsaTableTransaction> tableStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
		private final Comparator<TRowKey> comparator = new HashComparator();
		private NavigableMap<TRowKey, HaeinsaRowTransaction> mutationRowStates = null;
		private NavigableMap<TRowKey, HaeinsaRowTransaction> readOnlyRowStates = null;

		public NavigableMap<byte[], HaeinsaTableTransaction> getTableStates() {
			return tableStates;
		}

		/**
		 * Determine commitMethod among {@link CommitMethod#READ_ONLY},
		 * {@link CommitMethod#SINGLE_ROW_PUT_ONLY} and
		 * {@link CommitMethod#MULTI_ROW_MUTATIONS}
		 * <p>
		 * Transaction of single row with at least one of {@link HaeinsaDelete}
		 * will be considered as {@link CommitMethod#MULTI_ROW_MUTATIONS}.
		 *
		 * @return
		 */
		public CommitMethod determineCommitMethod() {
			int count = 0;
			boolean haveMuations = false;
			CommitMethod method = CommitMethod.NOTHING;
			for (HaeinsaTableTransaction tableState : tableStates.values()) {
				for (HaeinsaRowTransaction rowState : tableState.getRowStates().values()) {
					count++;
					if (rowState.getMutations().size() > 0) {
						// if any rowTx in Tx contains mutation ( Put/Delete )
						haveMuations = true;
					}

					if (count == 1) {
						if (rowState.getMutations().size() <= 0) {
							method = CommitMethod.READ_ONLY;
						} else if (rowState.getMutations().get(0) instanceof HaeinsaPut
								&& rowState.getMutations().size() == 1) {
							method = CommitMethod.SINGLE_ROW_PUT_ONLY;
						} else if (haveMuations) {
							// if rowTx contiains HaeinsaDelete
							method = CommitMethod.MULTI_ROW_MUTATIONS;
						}
					}
					if (count > 1) {
						if (haveMuations) {
							return CommitMethod.MULTI_ROW_MUTATIONS;
						} else {
							method = CommitMethod.READ_ONLY;
						}
					}
				}
			}
			return method;
		}

		/**
		 * Return mutation rows which is hash-sorted by TRowKey(table, row).
		 *
		 * @return
		 */
		public NavigableMap<TRowKey, HaeinsaRowTransaction> getMutationRowStates() {
			Preconditions.checkNotNull(mutationRowStates, "Should call classifyAndSortRows first.");
			return mutationRowStates;
		}

		/**
		 * Return read-only rows which is hash-sorted by TRowKey(table, row).
		 *
		 * @return
		 */
		public NavigableMap<TRowKey, HaeinsaRowTransaction> getReadOnlyRowStates() {
			Preconditions.checkNotNull(readOnlyRowStates, "Should call classifyAndSortRows first.");
			return readOnlyRowStates;
		}

		public void classifyAndSortRows(boolean onRecovery) {
			mutationRowStates = Maps.newTreeMap(comparator);
			readOnlyRowStates = Maps.newTreeMap(comparator);
			if (!onRecovery) {
				for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
					for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
						HaeinsaRowTransaction rowState = rowStateEntry.getValue();
						TRowKey rowKey = new TRowKey();
						rowKey.setTableName(tableStateEntry.getKey());
						rowKey.setRow(rowStateEntry.getKey());
						if (rowState.getMutations().size() > 0) {
							mutationRowStates.put(rowKey, rowState);
						} else {
							readOnlyRowStates.put(rowKey, rowState);
						}
					}
				}
			} else {
				for (Entry<byte[], HaeinsaTableTransaction> tableStateEntry : tableStates.entrySet()) {
					for (Entry<byte[], HaeinsaRowTransaction> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()) {
						HaeinsaRowTransaction rowState = rowStateEntry.getValue();
						TRowKey rowKey = new TRowKey();
						rowKey.setTableName(tableStateEntry.getKey());
						rowKey.setRow(rowStateEntry.getKey());
						mutationRowStates.put(rowKey, rowState);
					}
				}
			}
		}
	}

	/**
	 * Basic comparator which use lexicographical ordering of (byte[] table,
	 * byte[] row). Thread-safe & stateless
	 */
	private static class BasicComparator implements Comparator<TRowKey> {

		@Override
		public int compare(TRowKey o1, TRowKey o2) {
			return ComparisonChain
					.start()
					.compare(o1.getTableName(), o2.getTableName(), Bytes.BYTES_COMPARATOR)
					.compare(o1.getRow(), o2.getRow(), Bytes.BYTES_COMPARATOR)
					.result();
		}
	}

	/**
	 * Comparator which will deterministically order processing of each row.
	 * <p>
	 * Get guava murmur3_32bit hash value of (byte[] table, byte[] row), and
	 * compare those two to order {@link TRowKey}
	 */
	private static class HashComparator implements Comparator<TRowKey> {
		private static HashFunction HASH = Hashing.murmur3_32();
		private static Comparator<TRowKey> BASIC_COMP = new BasicComparator();

		@Override
		public int compare(TRowKey o1, TRowKey o2) {
			int hash1 = HASH.newHasher().putBytes(o1.getTableName()).putBytes(o1.getRow()).hash().asInt();
			int hash2 = HASH.newHasher().putBytes(o2.getTableName()).putBytes(o2.getRow()).hash().asInt();
			if (hash1 > hash2) {
				return 1;
			} else if (hash1 == hash2) {
				return BASIC_COMP.compare(o1, o2);
			} else {
				return -1;
			}
		}
	}
}
