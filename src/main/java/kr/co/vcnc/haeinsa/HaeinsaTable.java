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

import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_FAMILY;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_QUALIFIER;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.RECOVER_MAX_RETRY_COUNT;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_TIMEOUT;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import javax.annotation.Nullable;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.TRowLocks;
import kr.co.vcnc.haeinsa.thrift.generated.TCellKey;
import kr.co.vcnc.haeinsa.thrift.generated.TKeyValue;
import kr.co.vcnc.haeinsa.thrift.generated.TMutation;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Implementation of {@link HaeinsaTableIface}. It works with
 * {@link HaeinsaTransaction} to provide transaction on HBase.
 */
class HaeinsaTable implements HaeinsaTableIfaceInternal {
	private final HTableInterface table;

	public HaeinsaTable(HTableInterface table) {
		this.table = table;
	}

	@Override
	public byte[] getTableName() {
		return table.getTableName();
	}

	@Override
	public Configuration getConfiguration() {
		return table.getConfiguration();
	}

	@Override
	public HTableDescriptor getTableDescriptor() throws IOException {
		return table.getTableDescriptor();
	}

	/**
	 * Get data from HBase without transaction.
	 * {@link HaeinsaTransaction#commit()} to check or mutate lock column of the row scanned by this method.
	 * This method can be used when read performance is important or strict consistency of the result is not matter.
	 *
	 * @param get
	 * @return
	 * @throws IOException
	 */
	private HaeinsaResult getWithoutTx(HaeinsaGet get) throws IOException {
		Get hGet = new Get(get.getRow());
		for (Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap().entrySet()) {
			if (entry.getValue() == null) {
				hGet.addFamily(entry.getKey());
			} else {
				for (byte[] qualifier : entry.getValue()) {
					hGet.addColumn(entry.getKey(), qualifier);
				}
			}
		}
		Result result = table.get(hGet);
		return new HaeinsaResult(result);
	}

	@Override
	public HaeinsaResult get(@Nullable HaeinsaTransaction tx, HaeinsaGet get) throws IOException {
		Preconditions.checkNotNull(get);
		if (tx == null) {
			return getWithoutTx(get);
		}

		byte[] row = get.getRow();
		HaeinsaTableTransaction tableState = tx.createOrGetTableState(this.table.getTableName());
		HaeinsaRowTransaction rowState = tableState.getRowStates().get(row);
		boolean lockInclusive = false;
		Get hGet = new Get(get.getRow());

		for (Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap().entrySet()) {
			if (entry.getValue() == null) {
				hGet.addFamily(entry.getKey());
			} else {
				for (byte[] qualifier : entry.getValue()) {
					hGet.addColumn(entry.getKey(), qualifier);
				}
			}
		}

		if (rowState == null) {
			if (hGet.hasFamilies()) {
				hGet.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
			}
			lockInclusive = true;
		}

		Result result = table.get(hGet);
		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();
		if (rowState != null) {
			scanners.addAll(rowState.getScanners());
		}
		scanners.add(new HBaseGetScanner(result, Long.MAX_VALUE));

		HaeinsaResult hResult = null;
		// Scanners at this moment is:
		// union( muationScanners from RowTransaction, Scanner of get)
		try (ClientScanner scanner = new ClientScanner(tx, scanners, get.getFamilyMap(), lockInclusive)) {
			hResult = scanner.next();
		}
		if (hResult == null) {
			/*
			 * if specific row is empty and there was no puts at all, initialize ClientScanner make empty scanners variable.
			 * There will be no rowState associated to the row, and transaction will not operate normally.
			 * Therefore, create rowState if there was no HBase operation accessed to the row before.
			 */
			rowState = tableState.createOrGetRowState(row);
			if (rowState.getCurrent() == null) {
				rowState.setCurrent(TRowLocks.deserialize(null));
			}

			List<HaeinsaKeyValue> emptyList = Collections.emptyList();
			hResult = new HaeinsaResult(emptyList);
		}
		return hResult;
	}

	/**
	 * Scan data from HBase without transaction.
	 * {@link HaeinsaTransaction#commit()} to check or mutate lock column of the row scanned by this method.
	 * This method can be used when read performance is important or strict consistency of the result is not matter.
	 *
	 * @param scan
	 * @return
	 * @throws IOException IOException from HBase.
	 */
	private HaeinsaResultScanner getScannerWithoutTx(HaeinsaScan scan) throws IOException {
		Scan hScan = new Scan(scan.getStartRow(), scan.getStopRow());
		hScan.setCaching(scan.getCaching());
		hScan.setCacheBlocks(scan.getCacheBlocks());

		for (Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
			if (entry.getValue() == null) {
				hScan.addFamily(entry.getKey());
			} else {
				for (byte[] qualifier : entry.getValue()) {
					hScan.addColumn(entry.getKey(), qualifier);
				}
			}
		}
		final ResultScanner scanner = table.getScanner(hScan);
		return new SimpleClientScanner(scanner);
	}

	/**
	 * Scan data inside single row (intraScan) without transaction.
	 * {@link HaeinsaTransaction#commit()} to check or mutate lock column of the row scanned by this method.
	 * This method can be used when read performance is important or strict consistency of the result is not matter.
	 *
	 * @param intraScan
	 * @return
	 * @throws IOException IOException from HBase.
	 */
	private HaeinsaResultScanner getScannerWithoutTx(HaeinsaIntraScan intraScan) throws IOException {
		Scan hScan = new Scan(intraScan.getRow(), Bytes.add(intraScan.getRow(), new byte[]{0x00}));
		hScan.setBatch(intraScan.getBatch());

		for (byte[] family : intraScan.getFamilies()) {
			hScan.addFamily(family);
		}

		ColumnRangeFilter rangeFilter = new ColumnRangeFilter(
				intraScan.getMinColumn(), intraScan.isMinColumnInclusive(),
				intraScan.getMaxColumn(), intraScan.isMaxColumnInclusive());
		hScan.setFilter(rangeFilter);

		final ResultScanner scanner = table.getScanner(hScan);
		return new SimpleClientScanner(scanner);
	}

	/**
	 * Haeinsa implementation of {@link Scan}.
	 * Scan range of row inside defined by {@link HaeinsaScan} in the context of transaction(tx).
	 * Return {@link #ClientScanner} which related to {@link HaeinsaTable} and {@link HaeinsaTransaction}.
	 * <p>
	 * Return {@link SimpleClientScanner} which do not support transaction if tx is null.
	 * User can use this feature if specific scan operation does not require strong consistency
	 * or high performance is crucial because scan too wide range of rows.
	 */
	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, HaeinsaScan scan) throws IOException {
		Preconditions.checkNotNull(scan);
		if (tx == null) {
			return getScannerWithoutTx(scan);
		}

		Scan hScan = new Scan(scan.getStartRow(), scan.getStopRow());
		hScan.setCaching(scan.getCaching());
		hScan.setCacheBlocks(scan.getCacheBlocks());

		for (Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
			if (entry.getValue() == null) {
				hScan.addFamily(entry.getKey());
			} else {
				for (byte[] qualifier : entry.getValue()) {
					hScan.addColumn(entry.getKey(), qualifier);
				}
			}
		}
		if (hScan.hasFamilies()) {
			hScan.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
		}

		HaeinsaTableTransaction tableState = tx.createOrGetTableState(getTableName());
		NavigableMap<byte[], HaeinsaRowTransaction> rows;

		if (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)) {
			if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
				// null, null
				rows = tableState.getRowStates();
			} else {
				// null, StopRow
				rows = tableState.getRowStates().headMap(scan.getStopRow(), false);
			}
		} else {
			if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
				// StartRow, null
				rows = tableState.getRowStates().tailMap(scan.getStartRow(), true);
			} else {
				// StartRow, StopRow
				rows = tableState.getRowStates().subMap(scan.getStartRow(), true, scan.getStopRow(), false);
			}
		}

		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();

		for (HaeinsaRowTransaction rowTx : rows.values()) {
			scanners.addAll(rowTx.getScanners());
		}
		scanners.add(new HBaseScanScanner(table.getScanner(hScan)));

		// Scanners at this moment is:
		// union( muationScanners from all RowTransactions, Scanner of scan )
		return new ClientScanner(tx, scanners, scan.getFamilyMap(), true);
	}

	/**
	 * Haeinsa implementation of {@link ColumnRangeFilter}.
	 * Scan range of column inside single row defined by {@link HaeinsaIntraScan} in the context of transaction(tx).
	 * Return {@link #ClientScanner} which related to {@link HaeinsaTable} and {@link HaeinsaTransaction}.
	 * <p>
	 * Return {@link SimpleClientScanner} which do not support transaction if tx is null.
	 * User can use this feature if specific intra-scan operation does not require strong consistency.
	 * <p>
	 * Haeinsa does not support column-range scan over multiple row in this version.
	 */
	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, HaeinsaIntraScan intraScan)
			throws IOException {
		Preconditions.checkNotNull(intraScan);
		if (tx == null) {
			return getScannerWithoutTx(intraScan);
		}

		// scan from startRow ( inclusive ) to startRow + 0x00 ( exclusive )
		Scan hScan = new Scan(intraScan.getRow(), Bytes.add(intraScan.getRow(), new byte[] { 0x00 }));
		hScan.setBatch(intraScan.getBatch());

		for (byte[] family : intraScan.getFamilies()) {
			hScan.addFamily(family);
		}

		ColumnRangeFilter rangeFilter = new ColumnRangeFilter(
				intraScan.getMinColumn(), intraScan.isMinColumnInclusive(),
				intraScan.getMaxColumn(), intraScan.isMaxColumnInclusive());
		hScan.setFilter(rangeFilter);

		HaeinsaTableTransaction tableState = tx.createOrGetTableState(getTableName());
		HaeinsaRowTransaction rowState = tableState.getRowStates().get(intraScan.getRow());
		if (rowState == null) {
			rowState = checkOrRecoverLock(tx, intraScan.getRow(), tableState, rowState);
		}

		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();
		if (rowState != null) {
			scanners.addAll(rowState.getScanners());
		}
		scanners.add(new HBaseScanScanner(table.getScanner(hScan)));

		// scanners at this moment is:
		// union( muationScanners from RowTransaction, Scanner of intraScan )
		return new ClientScanner(tx, scanners, hScan.getFamilyMap(), intraScan, false);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family) throws IOException {
		Preconditions.checkNotNull(family);

		HaeinsaScan scan = new HaeinsaScan();
		scan.addFamily(family);
		return getScanner(tx, scan);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family, byte[] qualifier)
			throws IOException {
		Preconditions.checkNotNull(family);
		Preconditions.checkNotNull(qualifier);

		HaeinsaScan scan = new HaeinsaScan();
		scan.addColumn(family, qualifier);
		return getScanner(tx, scan);
	}

	/**
	 * Check whether specific row need recover by checking {@link TRowLock}.
	 * Return true only if rowLock is NOT in {@link TRowLockState#STABLE} state and lock is expired.
	 * Return false if rowLock is in {@link TRowLockState#STABLE} state.
	 * Throw {@link ConflictException} if rowLock is not in stable state and not expired yet.
	 *
	 * @param rowLock
	 * @return true - when lock is established but expired. / false - when there
	 *         is no lock ( {@link TRowLockState#STABLE} )
	 * @throws IOException {@link ConflictException} if lock is established and
	 *         not expired.
	 */
	private boolean checkAndIsShouldRecover(TRowLock rowLock) throws IOException {
		if (rowLock.getState() != TRowLockState.STABLE) {
			if (rowLock.isSetExpiry() && rowLock.getExpiry() < System.currentTimeMillis()) {
				return true;
			}
			throw new ConflictException("this row is unstable and not expired yet.");
		}
		return false;
	}

	/**
	 * Call {@link HaeinsaTransaction#recover()}.
	 * Abort or recover when there is failed transaction on the row,
	 * throw {@link ConflictException} when there is ongoing transaction.
	 *
	 * @param tx
	 * @param row
	 * @throws IOException ConflictException, HBase IOException
	 */
	private void recover(HaeinsaTransaction tx, byte[] row) throws IOException {
		HaeinsaTransaction previousTx = tx.getManager().getTransaction(getTableName(), row);
		if (previousTx != null) {
			// 해당 row 에 아직 종료되지 않은 Transaction 이 남아 있는 경우
			previousTx.recover();
		}
	}

	@Override
	public void put(HaeinsaTransaction tx, HaeinsaPut put) throws IOException {
		Preconditions.checkNotNull(tx);
		Preconditions.checkNotNull(put);

		byte[] row = put.getRow();
		HaeinsaTableTransaction tableState = tx.createOrGetTableState(this.table.getTableName());
		HaeinsaRowTransaction rowState = tableState.getRowStates().get(row);
		if (rowState == null) {
			// TODO(improvement) : Should consider to get lock when commit() called.
			rowState = checkOrRecoverLock(tx, row, tableState, rowState);
		}
		rowState.addMutation(put);
	}

	/**
	 * Check rowState and whether it contains {@link TRowLock} already, create one if not.
	 * <p>
	 * Get {@link TRowLock} of the row from HBase if rowState does not contains it.
	 * If lock is not in stable state, try to recover it first by {@link HaeinsaTrasaction#recover()}.
	 * <p>
	 * By calling this method proper time, {@link RowTransaction} inside {@link HaeinsaTransaction} can have
	 * {@link TRowLock} of the row when this method was called first time in the context of the transaction.
	 *
	 * @param tx
	 * @param row
	 * @param tableState
	 * @param rowState
	 * @return
	 * @throws IOException ConflictionException, HBase IOException
	 */
	private HaeinsaRowTransaction checkOrRecoverLock(HaeinsaTransaction tx,
			byte[] row, HaeinsaTableTransaction tableState,
			@Nullable HaeinsaRowTransaction rowState) throws IOException {
		if (rowState != null && rowState.getCurrent() != null) {
			// return rowState itself if rowState already exist and contains TRowLock
			return rowState;
		}
		int recoverCount = 0;
		while (true) {
			if (recoverCount > HaeinsaConstants.RECOVER_MAX_RETRY_COUNT) {
				throw new ConflictException("recover retry count is exceeded.");
			}
			TRowLock currentRowLock = getRowLock(row);
			if (checkAndIsShouldRecover(currentRowLock)) {
				recover(tx, row);
				recoverCount++;
			} else {
				rowState = tableState.createOrGetRowState(row);
				rowState.setCurrent(currentRowLock);
				break;
			}
		}
		return rowState;
	}

	@Override
	public void put(HaeinsaTransaction tx, List<HaeinsaPut> puts) throws IOException {
		Preconditions.checkNotNull(tx);
		Preconditions.checkNotNull(puts);

		for (HaeinsaPut put : puts) {
			put(tx, put);
		}
	}

	@Override
	public void delete(HaeinsaTransaction tx, HaeinsaDelete delete) throws IOException {
		Preconditions.checkNotNull(tx);
		Preconditions.checkNotNull(delete);

		byte[] row = delete.getRow();
		// Can't delete entire row in Haeinsa because of lock column. Please specify column families when needed.
		Preconditions.checkArgument(delete.getFamilyMap().size() > 0, "can't delete an entire row.");
		HaeinsaTableTransaction tableState = tx.createOrGetTableState(this.table.getTableName());
		HaeinsaRowTransaction rowState = tableState.getRowStates().get(row);
		if (rowState == null) {
			// TODO(improvement) : Should consider to get lock when commit() called.
			rowState = checkOrRecoverLock(tx, row, tableState, rowState);
		}
		rowState.addMutation(delete);
	}

	@Override
	public void delete(HaeinsaTransaction tx, List<HaeinsaDelete> deletes) throws IOException {
		Preconditions.checkNotNull(tx);
		Preconditions.checkNotNull(deletes);

		for (HaeinsaDelete delete : deletes) {
			delete(tx, delete);
		}
	}

	@Override
	public void close() throws IOException {
		table.close();
	}

	@Override
	public void commitSingleRowPutOnly(HaeinsaRowTransaction rowState, byte[] row) throws IOException {
		HaeinsaTransaction tx = rowState.getTableTransaction().getTransaction();
		Put put = new Put(row);
		HaeinsaPut haeinsaPut = (HaeinsaPut) rowState.getMutations().remove(0);
		for (HaeinsaKeyValue kv : Iterables.concat(haeinsaPut.getFamilyMap().values())) {
			put.add(kv.getFamily(), kv.getQualifier(), tx.getCommitTimestamp(), kv.getValue());
		}
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, tx.getCommitTimestamp());
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getCommitTimestamp(), TRowLocks.serialize(newRowLock));

		byte[] currentRowLockBytes = TRowLocks.serialize(rowState.getCurrent());
		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
			throw new ConflictException("can't acquire row's lock, commitSingleRowPutOnly failed");
		} else {
			rowState.setCurrent(newRowLock);
		}
	}

	/**
	 * Read {@link TRowLock} from HBase and compare that lock with prevRowLock.
	 * If TRowLock is changed, it means transaction is failed, so throw
	 * {@link ConflictException}.
	 *
	 * @param prevRowLock
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 * @throws NullPointException if oldLock is null (haven't read lock from
	 *         HBase)
	 */
	@Override
	public void checkSingleRowLock(HaeinsaRowTransaction rowState, byte[] row) throws IOException {
		TRowLock currentRowLock = getRowLock(row);
		if (!rowState.getCurrent().equals(currentRowLock)) {
			throw new ConflictException("this row is modified, checkSingleRow failed");
		}
	}

	@Override
	public void prewrite(HaeinsaRowTransaction rowState, byte[] row, boolean isPrimary) throws IOException {
		Put put = new Put(row);
		Set<TCellKey> prewritten = Sets.newTreeSet();
		// order of remaining as TRemove, TPut, TRemove, TPut, ...
		List<TMutation> remaining = Lists.newArrayList();
		HaeinsaTransaction tx = rowState.getTableTransaction().getTransaction();
		if (rowState.getMutations().size() > 0) {
			if (rowState.getMutations().get(0) instanceof HaeinsaPut) {
				HaeinsaPut haeinsaPut = (HaeinsaPut) rowState.getMutations().remove(0);
				for (HaeinsaKeyValue kv : Iterables.concat(haeinsaPut.getFamilyMap().values())) {
					put.add(kv.getFamily(), kv.getQualifier(), tx.getPrewriteTimestamp(), kv.getValue());
					TCellKey cellKey = new TCellKey();
					cellKey.setFamily(kv.getFamily());
					cellKey.setQualifier(kv.getQualifier());
					prewritten.add(cellKey);
				}
			}
			for (HaeinsaMutation mutation : rowState.getMutations()) {
				remaining.add(mutation.toTMutation());
			}
		}

		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.PREWRITTEN,
				tx.getCommitTimestamp()).setCurrentTimestmap(tx.getPrewriteTimestamp());
		if (isPrimary) {
			// for primary row
			for (Entry<TRowKey, HaeinsaRowTransaction> rowStateEntry : tx.getMutationRowStates().entrySet()) {
				TRowKey rowKey = rowStateEntry.getKey();
				if (Bytes.equals(rowKey.getTableName(), getTableName()) && Bytes.equals(rowKey.getRow(), row)) {
					// if this is primaryRow
					continue;
				}
				newRowLock.addToSecondaries(new TRowKey().setTableName(rowKey.getTableName()).setRow(rowKey.getRow()));
			}
		} else {
			// for secondary rows
			newRowLock.setPrimary(tx.getPrimary());
		}

		newRowLock.setPrewritten(Lists.newArrayList(prewritten));
		newRowLock.setMutations(remaining);
		newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getPrewriteTimestamp(), TRowLocks.serialize(newRowLock));

		byte[] currentRowLockBytes = TRowLocks.serialize(rowState.getCurrent());

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
			// Consider as conflict because another transaction might acquire lock of this row.
			tx.abort();
			throw new ConflictException("can't acquire row's lock");
		} else {
			rowState.setCurrent(newRowLock);
		}
	}

	@Override
	public void applyMutations(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		if (rowTxState.getCurrent().getMutationsSize() == 0) {
			// If this row does not have any left mutations to apply.
			return;
		}

		List<TMutation> remaining = Lists.newArrayList(rowTxState.getCurrent().getMutations());
		long currentTimestamp = rowTxState.getCurrent().getCurrentTimestmap();

		for (int i = 0; i < remaining.size(); i++) {
			byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState.getCurrent());
			int mutationOffset = i + 1;
			long mutationTimestamp = currentTimestamp + mutationOffset;

			TMutation mutation = remaining.get(i);
			switch (mutation.getType()) {
			case PUT: {
				TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
				newRowLock.setCurrentTimestmap(mutationTimestamp);
				newRowLock.setMutations(remaining.subList(mutationOffset, remaining.size()));
				// Maintain prewritten state and extend lock by ROW_LOCK_TIMEOUT
				newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
				Put put = new Put(row);
				put.add(LOCK_FAMILY, LOCK_QUALIFIER, newRowLock.getCurrentTimestmap(), TRowLocks.serialize(newRowLock));
				for (TKeyValue kv : mutation.getPut().getValues()) {
					put.add(kv.getKey().getFamily(), kv.getKey().getQualifier(), newRowLock.getCurrentTimestmap(), kv.getValue());
				}
				if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
					// Consider as conflict because another transaction might acquire lock of this row.
					throw new ConflictException("can't acquire row's lock");
				} else {
					rowTxState.setCurrent(newRowLock);
				}
				break;
			}
			case REMOVE: {
				Delete delete = new Delete(row);
				if (mutation.getRemove().getRemoveFamiliesSize() > 0) {
					for (ByteBuffer removeFamily : mutation.getRemove().getRemoveFamilies()) {
						delete.deleteFamily(removeFamily.array(), mutationTimestamp);
					}
				}
				if (mutation.getRemove().getRemoveCellsSize() > 0) {
					for (TCellKey removeCell : mutation.getRemove().getRemoveCells()) {
						delete.deleteColumns(removeCell.getFamily(), removeCell.getQualifier(), mutationTimestamp);
					}
				}
				if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, delete)) {
					// Consider as conflict because another transaction might acquire lock of this row.
					throw new ConflictException("can't acquire row's lock");
				}
				break;
			}
			default:
				break;
			}
		}
	}

	@Override
	public void makeStable(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState.getCurrent());
		HaeinsaTransaction transaction = rowTxState.getTableTransaction().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, commitTimestamp);
		byte[] newRowLockBytes = TRowLocks.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, commitTimestamp, newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
			// Consider as success because another transaction might already stabilize this row.
		} else {
			rowTxState.setCurrent(newRowLock);
		}
	}

	@Override
	public void commitPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState.getCurrent());
		HaeinsaTransaction transaction = rowTxState.getTableTransaction().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(TRowLockState.COMMITTED);
		newRowLock.setCurrentTimestmap(newRowLock.getCurrentTimestmap() + 1);
		// make prewritten to null
		newRowLock.setPrewrittenIsSet(false);
		// extend expiry by ROW_LOCK_TIMEOUT
		newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);

		byte[] newRowLockBytes = TRowLocks.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, newRowLock.getCurrentTimestmap(), newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
			transaction.abort();
			// Consider as conflict because another transaction might acquire lock of primary row.
			throw new ConflictException("can't acquire primary row's lock");
		} else {
			rowTxState.setCurrent(newRowLock);
		}
	}

	@Override
	public TRowLock getRowLock(byte[] row) throws IOException {
		Get get = new Get(row);
		get.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
		Result result = table.get(get);
		if (result.isEmpty()) {
			return TRowLocks.deserialize(null);
		} else {
			byte[] rowLockBytes = result.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
			return TRowLocks.deserialize(rowLockBytes);
		}
	}

	@Override
	public void abortPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState.getCurrent());
		HaeinsaTransaction transaction = rowTxState.getTableTransaction().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setCurrentTimestmap(Math.min(newRowLock.getCommitTimestamp(), newRowLock.getCurrentTimestmap() + 1));
		newRowLock.setState(TRowLockState.ABORTED);
		newRowLock.setMutationsIsSet(false);
		newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);

		byte[] newRowLockBytes = TRowLocks.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, newRowLock.getCurrentTimestmap(), newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
			// Consider as conflict because another transaction might acquire lock of primary row.
			throw new ConflictException("can't acquire primary row's lock");
		} else {
			rowTxState.setCurrent(newRowLock);
		}
	}

	@Override
	public void deletePrewritten(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		if (rowTxState.getCurrent().getPrewrittenSize() == 0) {
			// nothing to do
			return;
		}
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState.getCurrent());
		long prewriteTimestamp = rowTxState.getCurrent().getCurrentTimestmap();
		Delete delete = new Delete(row);
		for (TCellKey cellKey : rowTxState.getCurrent().getPrewritten()) {
			delete.deleteColumn(cellKey.getFamily(), cellKey.getQualifier(), prewriteTimestamp);
		}
		if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, delete)) {
			// Consider as conflict because another transaction might acquire lock of this row.
			throw new ConflictException("can't acquire primary row's lock");
		}
	}

	protected HTableInterface getHTable() {
		return table;
	}

	/**
	 * Implementation of {@link HaeinsaReulstScanner} which is used when scan without transaction.
	 */
	private class SimpleClientScanner implements HaeinsaResultScanner {
		private final ResultScanner scanner;

		public SimpleClientScanner(ResultScanner scanner) {
			this.scanner = scanner;
		}

		@Override
		public Iterator<HaeinsaResult> iterator() {
			return Iterators.transform(scanner.iterator(), new Function<Result, HaeinsaResult>() {
				@Override
				public HaeinsaResult apply(@Nullable Result result) {
					return new HaeinsaResult(result);
				}
			});
		}

		@Override
		public HaeinsaResult[] next(int nbRows) throws IOException {
			Result[] resultArray = scanner.next(nbRows);
			HaeinsaResult[] transformed = new HaeinsaResult[resultArray.length];
			for (int i = 0; i < resultArray.length; i++) {
				transformed[i] = new HaeinsaResult(resultArray[i]);
			}
			return transformed;
		}

		@Override
		public HaeinsaResult next() throws IOException {
			Result result = scanner.next();
			if (result != null) {
				return new HaeinsaResult(result);
			} else {
				return null;
			}
		}

		@Override
		public void close() {
			scanner.close();
		}
	}

	/**
	 * Contains scanners for single {@link HaeinsaTable} to help project puts/deletes to gets/scans in same transaction.
	 * <p>
	 * This projection of mutations is crucial to proper execution of transaction.
	 * For instance, consider single transaction such as T = {W1(X), R2(X), R3(Y), W4(X)}.
	 * Haeinsa does not write mutations on HBase until commit() is called.
	 * Therefore if R(X) get data only from HBase, R2(X) on transaction T cannot read data written by W1(X)
	 * and this is not expected behavior to programmers.
	 * Haeinsa resolves this problem by projecting buffered mutations in client to get/scan operations executed in same transaction.
	 */
	private class ClientScanner implements HaeinsaResultScanner {
		private final HaeinsaTransaction tx;
		private final HaeinsaTableTransaction tableState;
		private boolean initialized;
		private final NavigableSet<HaeinsaKeyValueScanner> scanners =
				Sets.newTreeSet(HaeinsaKeyValueScanner.COMPARATOR);
		private final List<HaeinsaKeyValueScanner> scannerList = Lists.newArrayList();
		// tracking delete of one specific row.
		private final HaeinsaDeleteTracker deleteTracker = new HaeinsaDeleteTracker();
		private final HaeinsaColumnTracker columnTracker;

		/**
		 * true if TRowLock inside scanners, false if TRowLock is already
		 * included inside tableState.getRowStates().get(row)
		 */
		private final boolean lockInclusive;

		/**
		 * -1 if not used. ( Get / Scan )
		 */
		private final int batch;
		private final Map<byte[], NavigableSet<byte[]>> familyMap;
		private HaeinsaKeyValue prevKV;
		private long maxSeqID = Long.MAX_VALUE;

		/**
		 *
		 * @param tx
		 * @param scanners
		 * @param familyMap
		 * @param lockInclusive - whether scanners contains {@link TRowLock} inside.
		 * 		  If not, should bring from {@link RowTransaction} or get from HBase directly.
		 */
		public ClientScanner(HaeinsaTransaction tx, Iterable<HaeinsaKeyValueScanner> scanners,
				Map<byte[], NavigableSet<byte[]>> familyMap, boolean lockInclusive) {
			this(tx, scanners, familyMap, null, lockInclusive);
		}

		/**
		 *
		 * @param tx
		 * @param scanners
		 * @param familyMap
		 * @param intraScan - To support to use {@link ColumnRangeFilter}
		 * @param lockInclusive - whether scanners contains {@link TRowLock} inside.
		 * 		  If not, should bring from {@link RowTransaction} or get from HBase directly.
		 */
		public ClientScanner(HaeinsaTransaction tx, Iterable<HaeinsaKeyValueScanner> scanners,
				Map<byte[], NavigableSet<byte[]>> familyMap, HaeinsaIntraScan intraScan, boolean lockInclusive) {
			this.tx = tx;
			this.tableState = tx.createOrGetTableState(getTableName());
			for (HaeinsaKeyValueScanner kvScanner : scanners) {
				scannerList.add(kvScanner);
			}
			if (intraScan == null) {
				intraScan = new HaeinsaIntraScan(null, null, false, null, false);
				intraScan.setBatch(-1);
			}
			this.columnTracker = new HaeinsaColumnTracker(familyMap,
					intraScan.getMinColumn(), intraScan.isMinColumnInclusive(),
					intraScan.getMaxColumn(), intraScan.isMaxColumnInclusive());
			this.batch = intraScan.getBatch();
			this.lockInclusive = lockInclusive;
			this.familyMap = familyMap;
		}

		/**
		 * Method to generate {@link #scanners} from {@link #scannerList}.
		 * Only can be called one time for every ClientScanner.
		 * <p>
		 * The reason why there are different variables for {@link #scannerList} and {@link #scanners} is that
		 * the way {@link #nextScanner()} implemented is removing {@link HaeinsaKeyValueScanner} one by one form scanners.
		 * {@link #close()} method needs to close every {@link ClientScanner} when called,
		 * so some other variable should preserve every scanner when ClientScanner created.
		 *
		 * @throws IOException
		 */
		private void initialize() throws IOException {
			try {
				for (HaeinsaKeyValueScanner scanner : scannerList) {
					HaeinsaKeyValue peeked = scanner.peek();
					if (peeked != null) {
						scanners.add(scanner);
					}
				}
				initialized = true;
			} catch (Exception e) {
				throw new IOException(e.getMessage(), e);
			}
		}

		/**
		 * Return iterator of {@link ClientScanner}. Use {@link #next()} inside.
		 */
		@Override
		public Iterator<HaeinsaResult> iterator() {
			return new Iterator<HaeinsaResult>() {
				// if current is null, whether scan is not started or next() was
				// called.
				// if hasNext() is called, next data will be ready on current.
				private HaeinsaResult current;

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}

				@Override
				public HaeinsaResult next() {
					if (current == null) {
						hasNext();
					}
					HaeinsaResult result = current;
					current = null;
					return result;
				}

				@Override
				public boolean hasNext() {
					if (current != null) {
						return true;
					}
					try {
						current = ClientScanner.this.next();
						if (current != null) {
							return true;
						}
					} catch (IOException e) {
						// because hasNext() cannot throw IOException, wrap it with RuntimeException.
						throw new IllegalStateException(e.getMessage(), e);
					}
					return false;
				}
			};
		}

		/**
		 * Return {@link TRowLock} for specific row from {@link IOException#scanners}.
		 * Return null if there is no proper {@link TRowLock}.
		 * <p>
		 * If one of these {@link #scanners} has row key which is smaller than given row,
		 * that {@link HaeinsaKeyValueScanner} is not peeked by this method and return null.
		 * So proper operation is guaranteed only when every scanner in {@link #scanners} return
		 * smaller or equal row key when {@link HaeinsaKeyValueScanner#peek()} is called.
		 *
		 * @param row
		 * @return null if there is no TRowLock information inside scanners,
		 *         return rowLock otherwise.
		 * @throws IOException
		 */
		private TRowLock peekLock(byte[] row) throws IOException {
			for (HaeinsaKeyValueScanner scanner : scanners) {
				HaeinsaKeyValue kv = scanner.peek();
				if (!Bytes.equals(kv.getRow(), row)) {
					break;
				}

				TRowLock rowLock = scanner.peekLock();
				if (rowLock != null) {
					return rowLock;
				}
			}
			return null;
		}

		@Override
		public HaeinsaResult next() throws IOException {
			if (!initialized) {
				// move scannerList -> scanners
				initialize();
			}

			final List<HaeinsaKeyValue> sortedKVs = Lists.newArrayList();
			while (true) {
				if (scanners.isEmpty()) {
					break;
				}
				HaeinsaKeyValueScanner currentScanner = scanners.first();
				HaeinsaKeyValue currentKV = currentScanner.peek();
				if (prevKV == null) {
					// start new row, deal with TRowLock and Recover()
					if (lockInclusive) {
						// HaeinsaKeyValues from HBaseScanScanner or HBaseGetScanner contains TRowLock for this row.
						TRowLock currentRowLock = peekLock(currentKV.getRow());
						HaeinsaRowTransaction rowState = tableState.createOrGetRowState(currentKV.getRow());
						if (rowState.getCurrent() == null) {
							// rowState is just created by createOrGetRowState method().
							// So proper TRowLock value should be set.
							if (currentRowLock == null) {
								/*
								 * HBase do not have TRowLock for this row.
								 * This is the case when Haeinsa accesses to this row first time.
								 * (This can be because of lazy-migration from HBase-only code).
								 *
								 * HaeinsaRowTransaction can use TRowLock(ROW_LOCK_VERSION,
								 * TRowLockState.STABLE, Long.MIN_VALUE) as initial TRowLock value.
								 * This initial TRowLock will be override by proper value and applied to HBase
								 * when commit() method is called.
								 */
								currentRowLock = TRowLocks.deserialize(null);
								rowState.setCurrent(currentRowLock);
							}

							if (checkAndIsShouldRecover(currentRowLock)) {
								// when currentRowLock is not stable but
								// expired.
								rowState = checkOrRecoverLock(tx, currentKV.getRow(), tableState, rowState);
								Get get = new Get(currentKV.getRow());
								for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
									if (entry.getValue() != null) {
										for (byte[] qualifier : entry.getValue()) {
											get.addColumn(entry.getKey(), qualifier);
										}
									} else {
										get.addFamily(entry.getKey());
									}
								}
								Result result = table.get(get);
								maxSeqID--;
								HBaseGetScanner getScanner = new HBaseGetScanner(result, maxSeqID);
								scanners.add(getScanner);
								continue;
							} else {
								// when currentRowLock is stable
								rowState.setCurrent(currentRowLock);
							}
						} else {
							// rowState is already exist, use current variable in rowState instead of TRowLock from scan or get
						}
						// At this point, TRowLock of currentKV.getRow() is saved in rowState.
					}
					prevKV = currentKV;
				}

				if (Bytes.equals(prevKV.getRow(), currentKV.getRow())) {
					if (currentScanner.getSequenceID() > maxSeqID) {
						// too old data, ignore
					} else if (Bytes.equals(currentKV.getFamily(), LOCK_FAMILY)
							&& Bytes.equals(currentKV.getQualifier(), LOCK_QUALIFIER)) {
						// if currentKV is Lock, ignore
					} else if (currentKV.getType() == Type.DeleteColumn || currentKV.getType() == Type.DeleteFamily) {
						// if currentKV is delete
						deleteTracker.add(currentKV, currentScanner.getSequenceID());
					} else if (prevKV == currentKV
							|| !(Bytes.equals(prevKV.getRow(), currentKV.getRow())
									&& Bytes.equals(prevKV.getFamily(), currentKV.getFamily())
									&& Bytes.equals(prevKV.getQualifier(), currentKV.getQualifier()))) {
						// if reference of prevKV and currentKV is same, the currentKV have new row.
						// Ignore when prevKV and currentKV is different but row, family,
						// qualifier of currentKv and prevKv is all same. (not likely)
						if (!deleteTracker.isDeleted(currentKV, currentScanner.getSequenceID())
								&& columnTracker.isMatched(currentKV)) {
							// if currentKV is not deleted and inside scan range
							sortedKVs.add(currentKV);
							prevKV = currentKV;
						}
					}
					nextScanner(currentScanner);
				} else {
					// currentKV is different row with prevKV, so reset
					// deleteTracker & maxSeqID
					deleteTracker.reset();
					prevKV = null;
					maxSeqID = Long.MAX_VALUE;
					if (sortedKVs.size() > 0) {
						// If currentKV moved to next row and there are more than one KV satisfy scan requirement,
						// should return HaeinsaResult which aggregate HaeinsaKeyValue for previous row.
						break;
					}
				}
				if (batch > 0 && sortedKVs.size() >= batch) {
					// if intraScan & sortedKVs have more elements than batch.
					break;
				}
			}
			if (sortedKVs.size() > 0) {
				return new HaeinsaResult(sortedKVs);
			} else {
				// scanners are exhausted.
				return null;
			}
		}

		/**
		 * Moving index of scanner of currentScanner by one. If there is no more
		 * element at the scanner, remove currentScanner from scanners
		 * (NavigableSet)
		 *
		 * @param currentScanner
		 * @throws IOException
		 */
		private void nextScanner(HaeinsaKeyValueScanner currentScanner) throws IOException {
			scanners.remove(currentScanner);
			currentScanner.next();
			HaeinsaKeyValue currentScannerNext = currentScanner.peek();
			if (currentScannerNext != null) {
				// if currentScanner is not exhausted.
				scanners.add(currentScanner);
			}
		}

		@Override
		public HaeinsaResult[] next(int nbRows) throws IOException {
			List<HaeinsaResult> result = Lists.newArrayList();
			for (int i = 0; i < nbRows; i++) {
				HaeinsaResult current = this.next();
				if (current != null) {
					result.add(current);
				} else {
					break;
				}
			}
			HaeinsaResult[] array = new HaeinsaResult[result.size()];
			return result.toArray(array);
		}

		@Override
		public void close() {
			for (HaeinsaKeyValueScanner scanner : scannerList) {
				scanner.close();
			}
		}
	}

	/**
	 * Implementation of {@link HaeinsaKeyValueScanner} which provides scanner interface which wrap
	 * {@link KeyValue}s from {@link ResultScanner} by HBase {@link Scan} to {@link HaeinsaKeyValue}s.
	 * This class is used if there are {@link HaeinsaScan} or {@link HaeinsaIntraScan} inside transaction.
	 * <p>
	 * HBaseScanScanner can contain kev-values for single row like {@link ResultScanner},
	 * or key-values of multiple rows.
	 * <p>
	 * HBaseScanScanner does not need sequenceID control because it will use {@link HBaseGetScanner}
	 * if rows from scan do not have stable state, and put result from get to lower sequenceID.
	 * Therefore, {@link #getSequenceID()} always return Long.MAX_VALUE
	 */
	private static class HBaseScanScanner implements HaeinsaKeyValueScanner {
		private final ResultScanner resultScanner;

		/**
		 * current is null when scan is not started or next() is called last
		 * time.
		 */
		private HaeinsaKeyValue current;

		/**
		 * currentResult is null when there is no more elements to scan in resultScanner or scan is not started.
		 * currentResult only contains result of single row.
		 */
		private Result currentResult;

		/**
		 * current = currentResult[resultIndex - 1]
		 */
		private int resultIndex;

		public HBaseScanScanner(ResultScanner resultScanner) {
			this.resultScanner = resultScanner;
		}

		@Override
		public HaeinsaKeyValue peek() {
			try {
				if (current != null) {
					return current;
				}
				if (currentResult == null || (currentResult != null && resultIndex >= currentResult.size())) {
					currentResult = resultScanner.next();
					if (currentResult != null && currentResult.isEmpty()) {
						currentResult = null;
					}
					resultIndex = 0;
				}
				if (currentResult == null) {
					// if currentResult is still null at this point, that means
					// there is no more KV to scan.
					return null;
				}
				// First scan or next() was called last time so move
				// resultIndex.
				current = new HaeinsaKeyValue(currentResult.raw()[resultIndex]);
				resultIndex++;

				return current;
			} catch (IOException e) {
				// because peek() cannot throw IOException, wrap it with RuntimeException.
				throw new IllegalStateException(e.getMessage(), e);
			}
		}

		@Override
		public HaeinsaKeyValue next() throws IOException {
			HaeinsaKeyValue result = peek();
			current = null;
			return result;
		}

		@Override
		public TRowLock peekLock() throws IOException {
			peek();
			if (currentResult != null) {
				byte[] lock = currentResult.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
				if (lock != null) {
					return TRowLocks.deserialize(lock);
				}
			}
			return null;
		}

		@Override
		public long getSequenceID() {
			/*
			 * Scan always read data from HBase, so sequenceId should be biggest every time.
			 */
			return Long.MAX_VALUE;
		}

		@Override
		public void close() {
			resultScanner.close();
		}
	}

	/**
	 * Implementation of {@link HaeinsaKeyValueScanner} which provide scanner interface which wrap
	 * {@link KeyValue}s from {@link ResultScanner} by HBase {@link Scan} to {@link HaeinsaKeyValue}s.
	 * This class is used there is {@link HaeinsaScan} or {@link HaeinsaIntraScan} inside transaction.
	 * <p>
	 * HBaseScanScanner can contain kev-values for single row like {@link ResultScanner},
	 * or key-values of multiple rows.
	 * <p>
	 * HBaseScanScanner does not need sequenceID control because it will use {@link HBaseGetScanner}
	 * if rows from scan do not have stable state, and put result from get to lower sequenceID.
	 * Therefore, {@link #getSequenceID()} always return Long.MAX_VALUE
	 */

	/**
	 * Implementation of {@link HaeinsaKeyValueScanner} which provides scanner interface which wrap
	 * {@link KeyValue}s from {@link Result} by HBase {@link Get} to {@link HaeinsaKeyValue}s.
	 * This class is used if there is {@link HaeinsaGet} inside transaction.
	 * <p>
	 * HBaseGetScanner can contain key-values for only single row link {@link Result}.
	 * So {@link #peek()} method or {@link #next()} method will return value from same row.
	 * <p>
	 * HBaseGetScanner is now used in two cases.
	 * First, inside {@link HaeinsaTable#get()}, {@link HaeinsaKeyValue}s returned by this class will
	 * always have sequenceId of Long.MAX_VALUE.
	 * Second is used inside {@link ClientScanner}. In this case, HBaseGetScanner recover failed transaction and
	 * get recovered data from HBase.
	 * Recovered {@link HaeinsaKeyValue} should have smaller sequenceId contained in ClientScanner so far
	 * because it is more recent value.
	 */
	private static class HBaseGetScanner implements HaeinsaKeyValueScanner {
		private final long sequenceID;
		private Result result;
		private int resultIndex;
		private HaeinsaKeyValue current;

		public HBaseGetScanner(Result result, final long sequenceID) {
			if (result != null && !result.isEmpty()) {
				this.result = result;
			} else {
				// null if result is empty
				this.result = null;
			}
			// bigger sequenceID is older one.
			this.sequenceID = sequenceID;
		}

		@Override
		public HaeinsaKeyValue peek() {
			if (current != null) {
				return current;
			}
			if (result != null && resultIndex >= result.size()) {
				result = null;
			}
			if (result == null) {
				return null;
			}
			current = new HaeinsaKeyValue(result.list().get(resultIndex));
			resultIndex++;
			return current;
		}

		@Override
		public HaeinsaKeyValue next() throws IOException {
			HaeinsaKeyValue result = peek();
			current = null;
			return result;
		}

		@Override
		public TRowLock peekLock() throws IOException {
			peek();
			if (result != null) {
				byte[] lock = result.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
				if (lock != null) {
					return TRowLocks.deserialize(lock);
				}
			}
			return null;
		}

		@Override
		public long getSequenceID() {
			return sequenceID;
		}

		@Override
		public void close() {
		}
	}
}
