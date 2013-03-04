package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_FAMILY;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_QUALIFIER;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class HaeinsaTable implements HaeinsaTableInterface {

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

	@Override
	public HaeinsaResult get(Transaction tx, HaeinsaGet get) throws IOException {
		byte[] row = get.getRow();
		TableTransaction tableState = tx.createOrGetTableState(this.table
				.getTableName());
		RowTransaction rowState = tableState.getRowStates().get(row);
		boolean lockInclusive = false;
		Get hGet = new Get(get.getRow());

		for (Entry<byte[], NavigableSet<byte[]>> entry : get.getFamilyMap()
				.entrySet()) {
			if (entry.getValue() == null) {
				hGet.addFamily(entry.getKey());
			} else {
				for (byte[] qualifier : entry.getValue()) {
					hGet.addColumn(entry.getKey(), qualifier);
				}
			}
		}
		if (rowState == null && hGet.hasFamilies()) {
			hGet.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
			lockInclusive = true;
		}

		Result result = table.get(hGet);
		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();
		if (rowState != null){
			scanners.addAll(rowState.getScanners());
		}
		scanners.add(new HBaseGetScanner(result, Long.MAX_VALUE));
		
		ClientScanner scanner = new ClientScanner(tx, scanners, get.getFamilyMap(), lockInclusive);
		HaeinsaResult hResult = scanner.next();
		scanner.close();
		if (hResult == null){
			List<HaeinsaKeyValue> emptyList = Collections.emptyList();
			hResult = new HaeinsaResultImpl(emptyList);
		}
		return hResult;
	}

	@Override
	public HaeinsaResultScanner getScanner(Transaction tx, HaeinsaScan scan)
			throws IOException {
		Scan hScan = new Scan(scan.getStartRow(), scan.getStopRow());
		hScan.setCaching(scan.getCaching());
		hScan.setCacheBlocks(scan.getCacheBlocks());

		for (Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap()
				.entrySet()) {
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

		TableTransaction tableState = tx.createOrGetTableState(getTableName());
		NavigableMap<byte[], RowTransaction> rows;

		if (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)) {
			if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
				rows = tableState.getRowStates();
			} else {
				rows = tableState.getRowStates().headMap(scan.getStopRow(),
						false);
			}
		} else {
			if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
				rows = tableState.getRowStates().tailMap(scan.getStartRow(),
						true);
			} else {
				rows = tableState.getRowStates().subMap(scan.getStartRow(),
						true, scan.getStopRow(), false);
			}
		}

		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();

		for (RowTransaction rowTx : rows.values()) {
			scanners.addAll(rowTx.getScanners());
		}
		scanners.add(new HBaseScanScanner(table.getScanner(hScan)));

		return new ClientScanner(tx, scanners, scan.getFamilyMap(), true);
	}
	
	@Override
	public HaeinsaResultScanner getScanner(Transaction tx,
			HaeinsaIntraScan intraScan) throws IOException {
		Scan hScan = new Scan(intraScan.getRow(), Bytes.add(intraScan.getRow(), new byte[]{ 0x00 }));
		hScan.setBatch(intraScan.getBatch());
		
		for (byte[] family : intraScan.getFamilies()) {
			hScan.addFamily(family);
		}
		
		ColumnRangeFilter rangeFilter = new ColumnRangeFilter(intraScan.getMinColumn(), intraScan.isMinColumnInclusive(), intraScan.getMaxColumn(), intraScan.isMaxColumnInclusive());
		hScan.setFilter(rangeFilter);

		TableTransaction tableState = tx.createOrGetTableState(getTableName());
		
		RowTransaction rowState = tableState.getRowStates().get(intraScan.getRow());
		
		if (rowState == null){
			rowState = checkOrRecoverLock(tx, intraScan.getRow(), tableState, rowState);
		}
		
		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();

		if (rowState != null){
			scanners.addAll(rowState.getScanners());
		}
		scanners.add(new HBaseScanScanner(table.getScanner(hScan)));

		return new ClientScanner(tx, scanners, hScan.getFamilyMap(), intraScan, false);
	}

	@Override
	public HaeinsaResultScanner getScanner(Transaction tx, byte[] family)
			throws IOException {
		HaeinsaScan scan = new HaeinsaScan();
		scan.addFamily(family);
		return getScanner(tx, scan);
	}

	@Override
	public HaeinsaResultScanner getScanner(Transaction tx, byte[] family,
			byte[] qualifier) throws IOException {
		HaeinsaScan scan = new HaeinsaScan();
		scan.addColumn(family, qualifier);
		return getScanner(tx, scan);
	}

	private boolean checkAndIsShouldRecover(TRowLock rowLock)
			throws IOException {
		if (rowLock.getState() != TRowLockState.STABLE) {
			if (rowLock.isSetExpiry()
					&& rowLock.getExpiry() < System.currentTimeMillis()) {
				return true;
			}
			throw new ConflictException("this row is unstable.");
		}
		return false;
	}

	private void recover(Transaction tx, byte[] row, TRowLock rowLock)
			throws IOException {
		Transaction previousTx = tx.getManager().getTransaction(getTableName(),
				row);
		if (previousTx != null){
			previousTx.recover();
		}
	}

	@Override
	public void put(Transaction tx, HaeinsaPut put) throws IOException {
		byte[] row = put.getRow();
		TableTransaction tableState = tx.createOrGetTableState(this.table
				.getTableName());
		RowTransaction rowState = tableState.getRowStates().get(row);
		if (rowState == null) {
			// TODO commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			rowState = checkOrRecoverLock(tx, row, tableState, rowState);
		}
		rowState.addMutation(put);
	}

	private RowTransaction checkOrRecoverLock(Transaction tx, byte[] row,
			TableTransaction tableState, RowTransaction rowState)
			throws IOException {
		if (rowState != null && rowState.getCurrent() != null){
			return rowState;
		}
		while (true){
			TRowLock currentRowLock = getRowLock(row);
			if (checkAndIsShouldRecover(currentRowLock)) {
				recover(tx, row, currentRowLock);
			}else{
				rowState = tableState.createOrGetRowState(row);
				rowState.setCurrent(currentRowLock);
				break;
			}
		}
		return rowState;
	}

	@Override
	public void put(Transaction tx, List<HaeinsaPut> puts) throws IOException {
		for (HaeinsaPut put : puts) {
			put(tx, put);
		}
	}

	@Override
	public void delete(Transaction tx, HaeinsaDelete delete) throws IOException {
		byte[] row = delete.getRow();
		// 전체 Row의 삭제는 불가능하다.
		Preconditions.checkArgument(delete.getFamilyMap().size() > 0,
				"can't delete an entire row.");
		TableTransaction tableState = tx.createOrGetTableState(this.table
				.getTableName());
		RowTransaction rowState = tableState.getRowStates().get(row);
		if (rowState == null) {
			rowState = checkOrRecoverLock(tx, row, tableState, rowState);
		}
		rowState.addMutation(delete);
	}

	@Override
	public void delete(Transaction tx, List<HaeinsaDelete> deletes)
			throws IOException {
		for (HaeinsaDelete delete : deletes) {
			delete(tx, delete);
		}
	}

	@Override
	public void close() throws IOException {
		table.close();
	}
	
	protected void commitSingleRowPutOnly(RowTransaction rowState, byte[] row) throws IOException{
		Transaction tx = rowState.getTableTransaction().getTransaction();
		Put put = new Put(row);
		HaeinsaPut haeinsaPut = (HaeinsaPut) rowState.getMutations().remove(0);
		for (HaeinsaKeyValue kv : Iterables.concat(haeinsaPut.getFamilyMap().values())){
			put.add(kv.getFamily(), kv.getQualifier(), tx.getCommitTimestamp(), kv.getValue());
		}
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, tx.getCommitTimestamp());
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getCommitTimestamp(), TRowLocks.serialize(newRowLock));
		
		byte[] currentRowLockBytes = TRowLocks.serialize(rowState
				.getCurrent());
		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
			throw new ConflictException("can't acquire row's lock");
		}else {
			rowState.setCurrent(newRowLock);
		}
	}
	
	protected void commitSingleRowReadOnly(RowTransaction rowState, byte[] row) throws IOException{
		TRowLock prevRowLock = rowState.getCurrent();
		TRowLock currentRowLock = getRowLock(row);
		if (!prevRowLock.equals(currentRowLock)){
			throw new ConflictException("this row is modified.");
		}
	}

	protected void prewrite(RowTransaction rowState, byte[] row, boolean isPrimary)
			throws IOException {
		Put put = new Put(row);
		Set<TCellKey> prewritten = Sets.newTreeSet();
		List<TMutation> remaining = Lists.newArrayList();
		Transaction tx = rowState.getTableTransaction().getTransaction();
		if (rowState.getMutations().size() > 0) {
			if (rowState.getMutations().get(0) instanceof HaeinsaPut) {
				HaeinsaPut haeinsaPut = (HaeinsaPut) rowState.getMutations()
						.remove(0);
				for (HaeinsaKeyValue kv : Iterables.concat(haeinsaPut
						.getFamilyMap().values())) {
					put.add(kv.getFamily(), kv.getQualifier(),
							tx.getPrewriteTimestamp(), kv.getValue());
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

		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION,
				TRowLockState.PREWRITTEN, tx.getCommitTimestamp())
				.setCurrentTimestmap(tx.getPrewriteTimestamp());
		if (isPrimary) {
			for (Entry<byte[], TableTransaction> tableStateEntry : tx
					.getTableStates().entrySet()) {
				for (Entry<byte[], RowTransaction> rowStateEntry : tableStateEntry
						.getValue().getRowStates().entrySet()) {
					if ((Bytes.equals(tableStateEntry.getKey(), getTableName()) && Bytes
							.equals(rowStateEntry.getKey(), row))) {
						continue;
					}
					newRowLock.addToSecondaries(new TRowKey().setTableName(
							tableStateEntry.getKey()).setRow(
							rowStateEntry.getKey()));
				}
			}
		} else {
			newRowLock.setPrimary(tx.getPrimary());
		}

		newRowLock.setPrewritten(Lists.newArrayList(prewritten));
		newRowLock.setMutations(remaining);
		newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getCommitTimestamp(),
				TRowLocks.serialize(newRowLock));

		byte[] currentRowLockBytes = TRowLocks.serialize(rowState
				.getCurrent());

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER,
				currentRowLockBytes, put)) {
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			tx.abort();
			throw new ConflictException("can't acquire row's lock");
		} else {
			rowState.setCurrent(newRowLock);
		}

	}

	protected void applyMutations(RowTransaction rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrent().getMutationsSize() == 0) {
			return;
		}

		List<TMutation> remaining = Lists.newArrayList(rowTxState.getCurrent()
				.getMutations());
		long currentTimestamp = rowTxState.getCurrent().getCurrentTimestmap();
		for (int i = 0; i < remaining.size(); i++) {
			byte[] currentRowLockBytes = TRowLocks
					.serialize(rowTxState.getCurrent());

			TMutation mutation = remaining.get(i);
			switch (mutation.getType()) {
			case PUT: {
				TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
				newRowLock.setCurrentTimestmap(currentTimestamp + i + 1);
				newRowLock.setMutations(remaining.subList(i + 1,
						remaining.size()));
				newRowLock.setExpiry(System.currentTimeMillis()
						+ ROW_LOCK_TIMEOUT);
				Put put = new Put(row);
				put.add(LOCK_FAMILY, LOCK_QUALIFIER,
						newRowLock.getCurrentTimestmap(),
						TRowLocks.serialize(newRowLock));
				for (TKeyValue kv : mutation.getPut().getValues()) {
					put.add(kv.getKey().getFamily(),
							kv.getKey().getQualifier(),
							newRowLock.getCurrentTimestmap(), kv.getValue());
				}
				if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER,
						currentRowLockBytes, put)) {
					// 실패하는 경우는 다른 쪽에서 row의 lock을 획득했으므로 충돌이 났다고 처리한다.
					throw new ConflictException("can't acquire row's lock");
				} else {
					rowTxState.setCurrent(newRowLock);
				}
				break;
			}

			case REMOVE: {
				Delete delete = new Delete(row);
				if (mutation.getRemove().getRemoveFamiliesSize() > 0){
					for (ByteBuffer removeFamily : mutation.getRemove()
							.getRemoveFamilies()) {
						delete.deleteFamily(removeFamily.array(), currentTimestamp
								+ i + 1);
					}
				}
				if (mutation.getRemove().getRemoveCellsSize() > 0){
					for (TCellKey removeCell : mutation.getRemove()
							.getRemoveCells()) {
						delete.deleteColumns(removeCell.getFamily(),
								removeCell.getQualifier(), currentTimestamp + i + 1);
					}
				}
				if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER,
						currentRowLockBytes, delete)) {
					// 실패하는 경우는 다른 쪽에서 row의 lock을 획득했으므로 충돌이 났다고 처리한다.
					throw new ConflictException("can't acquire row's lock");
				}
				break;
			}

			default:
				break;
			}
		}
	}

	/**
	 * make row from {@link TRowLockState#PREWRITTEN} or {@link TRowLockState#COMMITTED} or {@link TRowLockState#ABORTED} to {@link TRowLockState#STABLE}
	 * @param tx
	 * @param row
	 * @throws IOException
	 */
	protected void makeStable(RowTransaction rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState
				.getCurrent());
		Transaction transaction = rowTxState.getTableTransaction()
				.getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION,
				TRowLockState.STABLE, commitTimestamp);
		byte[] newRowLockBytes = TRowLocks.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, commitTimestamp, newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER,
				currentRowLockBytes, put)) {
			// 실패하는 경우는 다른 쪽에서 먼저 commit을 한 경우이므로 오류 없이 넘어가면 된다.
		} else {
			rowTxState.setCurrent(newRowLock);
		}
	}

	/**
	 * make primary row from {@link TRowLockState#PREWRITTEN} to {@link TRowLockState#COMMITTED}
	 * @param tx
	 * @param row
	 * @throws IOException
	 */
	protected void commitPrimary(RowTransaction rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState
				.getCurrent());
		Transaction transaction = rowTxState.getTableTransaction()
				.getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(TRowLockState.COMMITTED);
		newRowLock.setPrewrittenIsSet(false);
		newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);

		byte[] newRowLockBytes = TRowLocks.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, commitTimestamp, newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER,
				currentRowLockBytes, put)) {
			transaction.abort();
			// 실패하는 경우는 다른 쪽에서 row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		} else {
			rowTxState.setCurrent(newRowLock);
		}
	}

	/**
	 * get {@link TRowLock}
	 * @param row row
	 * @return row lock
	 * @throws IOException
	 */
	protected TRowLock getRowLock(byte[] row) throws IOException {
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

	/**
	 * make primary row from {@link TRowLockState#PREWRITTEN} to {@link TRowLockState#ABORTED}  
	 * @param tx
	 * @param row
	 * @throws IOException
	 */
	protected void abortPrimary(RowTransaction rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState
				.getCurrent());
		Transaction transaction = rowTxState.getTableTransaction()
				.getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(TRowLockState.ABORTED);
		newRowLock.setMutationsIsSet(false);
		newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);

		byte[] newRowLockBytes = TRowLocks.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, commitTimestamp, newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER,
				currentRowLockBytes, put)) {
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		} else {
			rowTxState.setCurrent(newRowLock);
		}
	}

	/**
	 * delete row's puts({@link TRowLock#prewritten}).
	 * @param rowTxState
	 * @param row
	 * @throws IOException
	 */
	protected void deletePrewritten(RowTransaction rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrent().getPrewrittenSize() == 0) {
			return;
		}
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState
				.getCurrent());
		long prewriteTimestamp = rowTxState.getCurrent().getCurrentTimestmap();
		Delete delete = new Delete(row);
		for (TCellKey cellKey : rowTxState.getCurrent().getPrewritten()) {
			delete.deleteColumn(cellKey.getFamily(), cellKey.getQualifier(),
					prewriteTimestamp);
		}
		if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER,
				currentRowLockBytes, delete)) {
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}
	}

	protected HTableInterface getHTable() {
		return table;
	}

	private class ClientScanner implements HaeinsaResultScanner { 
		private final Transaction tx;
		private final TableTransaction tableState;
		private boolean initialized = false;
		private final NavigableSet<HaeinsaKeyValueScanner> scanners = Sets
				.newTreeSet(HaeinsaKeyValueScanner.COMPARATOR);
		private final List<HaeinsaKeyValueScanner> scannerList = Lists
				.newArrayList();
		private final HaeinsaDeleteTracker deleteTracker = new HaeinsaDeleteTracker();
		private final HaeinsaColumnTracker columnTracker;
		private final boolean lockInclusive;
		private final int batch;
		private final Map<byte[], NavigableSet<byte[]>> familyMap; 
		private HaeinsaKeyValue prevKV = null;
		private long maxSeqID = Long.MAX_VALUE;
		
		public ClientScanner(Transaction tx,
				Iterable<HaeinsaKeyValueScanner> scanners,
				Map<byte[], NavigableSet<byte[]>> familyMap, boolean lockInclusive) {
			this(tx, scanners, familyMap, null, lockInclusive);
		}
		
		
		public ClientScanner(Transaction tx,
				Iterable<HaeinsaKeyValueScanner> scanners,
				Map<byte[], NavigableSet<byte[]>> familyMap, HaeinsaIntraScan intraScan, boolean lockInclusive) {
			this.tx = tx;
			this.tableState = tx.createOrGetTableState(getTableName());
			for (HaeinsaKeyValueScanner kvScanner : scanners) {
				scannerList.add(kvScanner);
			}
			if (intraScan == null){
				intraScan = new HaeinsaIntraScan(null, null, false, null, false);
				intraScan.setBatch(-1);
			}
			this.columnTracker = new HaeinsaColumnTracker(familyMap
					,intraScan.getMinColumn(), intraScan.isMinColumnInclusive()
					,intraScan.getMaxColumn(), intraScan.isMaxColumnInclusive());
			this.batch = intraScan.getBatch();
			this.lockInclusive = lockInclusive;
			this.familyMap = familyMap;
		}

		private void initialize() throws IOException {
			try {
				scanners.addAll(scannerList);
				initialized = true;
			} catch (Exception e) {
				throw new IOException(e.getMessage(), e);
			}
		}

		@Override
		public Iterator<HaeinsaResult> iterator() {
			return new Iterator<HaeinsaResult>() {
				private HaeinsaResult current = null;

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
						throw new IllegalStateException(e.getMessage(), e);
					}
					return false;
				}
			};
		}
		
		private TRowLock peekLock(byte[] row) throws IOException{
			
			for (HaeinsaKeyValueScanner scanner : scanners){
				HaeinsaKeyValue kv = scanner.peek();
				if (!Bytes.equals(kv.getRow(), row)){
					break;
				}
				TRowLock rowLock = scanner.peekLock();
				if (rowLock != null){
					return rowLock;
				}
			}
			return null;
		}

		@Override
		public HaeinsaResult next() throws IOException {
			if (!initialized) {
				initialize();
			}
			final List<HaeinsaKeyValue> kvs = Lists.newArrayList();
			
			while (true) {
				if (scanners.isEmpty()) {
					break;
				}
				HaeinsaKeyValueScanner currentScanner = scanners.first();
				HaeinsaKeyValue currentKV = currentScanner.peek();
				if (prevKV == null) {
					// start new row
					if (lockInclusive){
						TRowLock currentRowLock = peekLock(currentKV.getRow());
						RowTransaction rowState = tableState.createOrGetRowState(currentKV.getRow());
						if (rowState.getCurrent() == null){
							if (currentRowLock == null){
								currentRowLock = TRowLocks.deserialize(null);
								rowState.setCurrent(currentRowLock);
							}
							
							if (checkAndIsShouldRecover(currentRowLock)){
								rowState = checkOrRecoverLock(tx, currentKV.getRow(), tableState, rowState);
								Get get = new Get(currentKV.getRow());
								for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()){
									if (entry.getValue() != null){
										for (byte[] qualifier : entry.getValue()){
											get.addColumn(entry.getKey(), qualifier);
										}
									}else{
										get.addFamily(entry.getKey());
									}
								}
								Result result = table.get(get);
								maxSeqID --;
								HBaseGetScanner getScanner = new HBaseGetScanner(result, maxSeqID);
								scanners.add(getScanner);
								continue;
							}else{
								rowState.setCurrent(currentRowLock);
							}
						}
					}
					prevKV = currentKV;
				}
				
				if (Bytes.equals(prevKV.getRow(), currentKV.getRow())) {
					if (currentScanner.getSequenceID() > maxSeqID) {
						
					} else if (Bytes.equals(currentKV.getFamily(), LOCK_FAMILY) && Bytes.equals(currentKV.getQualifier(), LOCK_QUALIFIER)){
						
					} else if (currentKV.getType() == Type.DeleteColumn || currentKV.getType() == Type.DeleteFamily){
						deleteTracker.add(currentKV, currentScanner.getSequenceID());
					} else if (prevKV == currentKV
							|| !(Bytes.equals(prevKV.getRow(),
									currentKV.getRow())
									&& Bytes.equals(prevKV.getFamily(),
											currentKV.getFamily()) && Bytes
										.equals(prevKV.getQualifier(),
												currentKV.getQualifier()))) {
						// Row, Family, Qualifier 모두가 같은 경우가 더 나오면 무시한다.
						if (!deleteTracker.isDeleted(currentKV, currentScanner.getSequenceID()) && columnTracker.isMatched(currentKV)){
							kvs.add(currentKV);
							prevKV = currentKV;
						}
					}
					
					nextScanner(currentScanner);
				} else {
					deleteTracker.reset();
					prevKV = null;
					maxSeqID = Long.MAX_VALUE;
					if (kvs.size() > 0){
						break;
					}
				}
				if (batch > 0 && kvs.size() >= batch){
					break;
				}
			}
			if (kvs.size() > 0) {
				return new HaeinsaResultImpl(kvs);
			} else {
				return null;
			}
		}

		private void nextScanner(HaeinsaKeyValueScanner currentScanner)
				throws IOException {
			scanners.remove(currentScanner);
			currentScanner.next();
			HaeinsaKeyValue currentScannerNext = currentScanner.peek();
			if (currentScannerNext != null) {
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

	private static class HaeinsaResultImpl implements HaeinsaResult {
		private final List<HaeinsaKeyValue> kvs;
		private byte[] row = null;

		public HaeinsaResultImpl(List<HaeinsaKeyValue> kvs) {
			this.kvs = kvs;
			if (kvs.size() > 0) {
				row = kvs.get(0).getRow();
			}
		}

		@Override
		public byte[] getRow() {
			return row;
		}

		@Override
		public List<HaeinsaKeyValue> list() {
			return kvs;
		}

		@Override
		public byte[] getValue(byte[] family, byte[] qualifier) {
			int index = Collections.binarySearch(kvs, new HaeinsaKeyValue(row,
					family, qualifier, null, KeyValue.Type.Put),
					HaeinsaKeyValue.COMPARATOR);
			if (index >= 0) {
				return kvs.get(index).getValue();
			}
			return null;
		}

		@Override
		public boolean containsColumn(byte[] family, byte[] qualifier) {
			return getValue(family, qualifier) != null;
		}

		@Override
		public boolean isEmpty() {
			return kvs.size() == 0;
		}
	}

	private static class HBaseScanScanner implements
			HaeinsaKeyValueScanner {
		private final ResultScanner resultScanner;
		private HaeinsaKeyValue current;
		private Result currentResult;
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
				if (currentResult == null
						|| (currentResult != null && resultIndex >= currentResult
								.size())) {
					currentResult = resultScanner.next();
					if (currentResult != null && currentResult.isEmpty()) {
						currentResult = null;
					}
					resultIndex = 0;
				}
				if (currentResult == null) {
					return null;
				}
				current = new HaeinsaKeyValue(currentResult.raw()[
						resultIndex]);
				resultIndex++;

				return current;
			} catch (IOException e) {
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
			if (currentResult != null){
				byte[] lock = currentResult.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
				if (lock != null){
					return TRowLocks.deserialize(lock);
				}
			}
			return null;
		}

		@Override
		public long getSequenceID() {
			return Long.MAX_VALUE;
		}

		@Override
		public void close() {
			resultScanner.close();
		}
	}

	private static class HBaseGetScanner implements
			HaeinsaKeyValueScanner {
		private final long sequenceID;
		private Result result;
		private int resultIndex;
		private HaeinsaKeyValue current;
		
		public HBaseGetScanner(Result result, final long sequenceID) { 
			if (result != null && !result.isEmpty()) {
				this.result = result;
			} else {
				this.result = null;
			}
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
			if (result != null){
				byte[] lock = result.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
				if (lock != null){
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
