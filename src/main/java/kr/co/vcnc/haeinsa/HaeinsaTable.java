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
 * Implementation of {@link HaeinsaTableIface}. 
 * It works with {@link HaeinsaTransaction} to provide transaction on HBase. 
 * @author Youngmok Kim, Myungbo Kim
 *
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
	 * Transaction 을 사용하지 않고 get 을 할 수 있게 해준다.
	 * 이 명령을 통해서 읽어온 row 에 대해서는 HBase의 lock 을 확인하지 않는다.
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
		
		if (tx == null){
			return getWithoutTx(get);
		}
		
		byte[] row = get.getRow();
		HaeinsaTableTransaction tableState = tx.createOrGetTableState(this.table.getTableName());
		HaeinsaRowTransaction rowState = tableState.getRowStates().get(row);
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
		//	To ymkim
		//	rowState == null 이고 hGet 이 family 가 아예 없는 경우에도 여전히 lock 을 가져올 수 있지 않나?
		if (rowState == null) {
			if(hGet.hasFamilies()){
				hGet.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
			}
			lockInclusive = true;
		}

		Result result = table.get(hGet);
		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();
		if (rowState != null){
			scanners.addAll(rowState.getScanners());
		}
		scanners.add(new HBaseGetScanner(result, Long.MAX_VALUE));
		
		//	scanners at this moment = union( muationScanners from RowTransaction, Scanner of get )
		ClientScanner scanner = new ClientScanner(tx, scanners, get.getFamilyMap(), lockInclusive);
		HaeinsaResult hResult = scanner.next();
		scanner.close();
		if (hResult == null){
			// row가 비어있고 해당 row에 아무런 put도 하지 않았을 경우, ClientScanner를 initailize하면서 scanners에 들어있는 scanner가 아무것도 없게 됨
			// 그러므로, 해당 row에 RowState를 만들지 않아서 제대로 된 transaction 처리가 되지 않는다.
			// 그래서 여기서 RowState가 없다면 만들어준다.
			rowState = tableState.createOrGetRowState(row);
			if (rowState.getCurrent() == null){
				rowState.setCurrent(TRowLocks.deserialize(null));
			}
			
			List<HaeinsaKeyValue> emptyList = Collections.emptyList();
			hResult = new HaeinsaResult(emptyList);
		}
		return hResult;
	}
	
	/**
	 * Transaction 을 사용하지 않고 scan 을 할 수 있게 해준다.
	 * 이 명령을 통해서 읽어온 row 에 대해서는 HBase의 lock 을 확인하지 않는다.
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
	 * Transaction 을 사용하지 않고 intraScan 을 할 수 있게 해준다. 
	 * 이 명령을 통해서 읽어온 row 에 대해서는 HBase의 lock 을 확인하지 않는다.
	 * @param intraScan
	 * @return
	 * @throws IOException IOException from HBase.
	 */
	private HaeinsaResultScanner getScannerWithoutTx(HaeinsaIntraScan intraScan) throws IOException {
		Scan hScan = new Scan(intraScan.getRow(), Bytes.add(intraScan.getRow(), new byte[]{ 0x00 }));
		hScan.setBatch(intraScan.getBatch());
		
		for (byte[] family : intraScan.getFamilies()) {
			hScan.addFamily(family);
		}
		
		ColumnRangeFilter rangeFilter = 
				new ColumnRangeFilter(intraScan.getMinColumn(), intraScan.isMinColumnInclusive(), 
						intraScan.getMaxColumn(), intraScan.isMaxColumnInclusive());
		hScan.setFilter(rangeFilter);
		
		final ResultScanner scanner = table.getScanner(hScan);
		return new SimpleClientScanner(scanner);
	}

	/**
	 * {@link Scan} 의 Haeinsa implementation 이다. 
	 * scan 에 정의된 row 범위에 대한 스캔을 할 수 있다.
	 * 해당 HaeinsaTable 과 {@link HaeinsaTransaction} 에 연관된 {@link ClientScanner} 을 반환한다.
	 * <p>만약 tx 가 null 이면 Transaction 을 제공하지 않는 {@link SimpleClientScanner} 를 반환한다.
	 */
	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, HaeinsaScan scan)
			throws IOException {
		Preconditions.checkNotNull(scan);
		
		if (tx == null){
			return getScannerWithoutTx(scan);
		}
		
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

		HaeinsaTableTransaction tableState = tx.createOrGetTableState(getTableName());
		NavigableMap<byte[], HaeinsaRowTransaction> rows;

		if (Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)) {
			if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
				//	null, null
				rows = tableState.getRowStates();
			} else {
				//	null, StopRow
				rows = tableState.getRowStates().headMap(scan.getStopRow(),	false);
			}
		} else {
			if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
				//	StartRow, null
				rows = tableState.getRowStates().tailMap(scan.getStartRow(), true);
			} else {
				//	StartRow, StopRow
				rows = tableState.getRowStates().subMap(scan.getStartRow(),
						true, scan.getStopRow(), false);
			}
		}

		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();

		for (HaeinsaRowTransaction rowTx : rows.values()) {
			scanners.addAll(rowTx.getScanners());
		}
		scanners.add(new HBaseScanScanner(table.getScanner(hScan)));

		//	scanners at this moment = union( muationScanners from all RowTransactions, Scanner of scan )
		return new ClientScanner(tx, scanners, scan.getFamilyMap(), true);
	}
	
	/**
	 * {@link ColumnRangeFilter} 의 Haeinsa implementation 이다. 
	 * intraScan 에 정의된 단일 row 의 Column 들에 대한 스캔을 할 수가 있다.
	 * <p>{@link HaeinsaIntraScan} 을 입력으로 사용한다. 
	 * 해당 HaeinsaTable 과 {@link HaeinsaTransaction} 에 연관된 {@link ClientScanner} 을 반환한다.
	 * <p>만약 tx 가 null 이면 Transaction 을 제공하지 않는 {@link SimpleClientScanner} 를 반환한다.  
	 */
	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx,
			HaeinsaIntraScan intraScan) throws IOException {
		Preconditions.checkNotNull(intraScan);
		
		if (tx == null){
			return getScannerWithoutTx(intraScan);
		}
		
		//	scan from startRow ( inclusive ) to startRow + 0x00 ( exclusive )	
		Scan hScan = new Scan(intraScan.getRow(), Bytes.add(intraScan.getRow(), new byte[]{ 0x00 }));
		hScan.setBatch(intraScan.getBatch());
		
		for (byte[] family : intraScan.getFamilies()) {
			hScan.addFamily(family);
		}
				
		ColumnRangeFilter rangeFilter = 
				new ColumnRangeFilter(intraScan.getMinColumn(), intraScan.isMinColumnInclusive(), 
						intraScan.getMaxColumn(), intraScan.isMaxColumnInclusive());
		hScan.setFilter(rangeFilter);

		HaeinsaTableTransaction tableState = tx.createOrGetTableState(getTableName());
		
		HaeinsaRowTransaction rowState = tableState.getRowStates().get(intraScan.getRow());
		
		if (rowState == null){
			rowState = checkOrRecoverLock(tx, intraScan.getRow(), tableState, rowState);
		}
		
		List<HaeinsaKeyValueScanner> scanners = Lists.newArrayList();

		if (rowState != null){
			scanners.addAll(rowState.getScanners());
		}
		scanners.add(new HBaseScanScanner(table.getScanner(hScan)));

		//	scanners at this moment = union( muationScanners from RowTransaction, Scanner of intraScan )
		return new ClientScanner(tx, scanners, hScan.getFamilyMap(), intraScan, false);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family)
			throws IOException {
		Preconditions.checkNotNull(family);
		
		HaeinsaScan scan = new HaeinsaScan();
		scan.addFamily(family);
		return getScanner(tx, scan);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family,
			byte[] qualifier) throws IOException {
		Preconditions.checkNotNull(family);
		Preconditions.checkNotNull(qualifier);
		
		HaeinsaScan scan = new HaeinsaScan();
		scan.addColumn(family, qualifier);
		return getScanner(tx, scan);
	}

	/**
	 * TRowLock 을 검사해서 Transaction 을 그대로 진행해도 되는지 ( false ) 아니면
	 * 해당 TRowLock 에 대한 recover 가 필요한지 ( true ) 판별한다.
	 * 만약 이미 lock 이 걸려 있고 expiry 가 지나지 않았다면 {@link ConflictException} 을 throw 한다.
	 * @param rowLock
	 * @return
	 * 			true - when lock is established but expired.
	 * 			/ false - when there is no lock ( {@link TRowLockState#STABLE} )
	 * @throws IOException {@link ConflictException} if lock is established and not expired.
	 */
	private boolean checkAndIsShouldRecover(TRowLock rowLock)
			throws IOException {
		if (rowLock.getState() != TRowLockState.STABLE) {
			if (rowLock.isSetExpiry()
					&& rowLock.getExpiry() < System.currentTimeMillis()) {
				return true;
			}
			throw new ConflictException("this row is unstable and not expired yet.");
		}
		return false;
	}
	
	/**
	 * {@link HaeinsaTransaction#recover()} 를 부른다.
	 * <p>해당 row 에 실패한 Transaction 이 있는 경우 마무리하고, 아직 Transaction 이 진행되고 있는 경우 ConflictException 이 throw 된다. 
	 * @param tx
	 * @param row
	 * @throws IOException ConflictException, HBase IOException
	 */
	private void recover(HaeinsaTransaction tx, byte[] row)
			throws IOException {
		HaeinsaTransaction previousTx = tx.getManager().getTransaction(getTableName(), row);
		if (previousTx != null){
			//	해당 row 에 아직 종료되지 않은 Transaction 이 남아 있는 경우
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
			// TODO(Brad):commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			rowState = checkOrRecoverLock(tx, row, tableState, rowState);
		}
		rowState.addMutation(put);
	}

	/**
	 * rowState 를 검사해서 row 에 관한 정보를 이미 가지고 있는지 확인하고, 가지고 있지 않다면 새로 만든다.
	 * <p>해당 row 에 대한 TRowLock 정보도 확인하게 되는데, 만약 정보가 없다면 HBase 에서 읽어오게 된다. 
	 * 이 때 HBase 에 기록된 Lock 이 {@link TRowLockState#STABLE} 이 아니라면
	 * {@link HaeinsaTransaction#recover()} 를 통해서 복원하는 것을 시도한다.
	 * <p>이 과정을 통해서 {@link RowTransaction} 은 해당 row 에 대해서 가장 먼저 checkOrRecoverLock() 을 부를 때의 
	 * TRowLock 정보를 가지고 있게 된다.   
	 *   
	 * @param tx
	 * @param row
	 * @param tableState
	 * @param rowState
	 * @return
	 * @throws IOException ConflictionException, HBase IOException
	 */
	private HaeinsaRowTransaction checkOrRecoverLock(HaeinsaTransaction tx, byte[] row,
			HaeinsaTableTransaction tableState, @Nullable HaeinsaRowTransaction rowState)
			throws IOException {
		if (rowState != null && rowState.getCurrent() != null){
			//	return rowState itself if rowState already exist and contains lock information.
			return rowState;
		}
		while (true){
			TRowLock currentRowLock = getRowLock(row);
			if (checkAndIsShouldRecover(currentRowLock)) {
				recover(tx, row);
			}else{
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
		// 전체 Row의 삭제는 불가능하다.
		Preconditions.checkArgument(delete.getFamilyMap().size() > 0,
				"can't delete an entire row.");
		HaeinsaTableTransaction tableState = tx.createOrGetTableState(this.table.getTableName());
		HaeinsaRowTransaction rowState = tableState.getRowStates().get(row);
		if (rowState == null) {
			// TODO(Brad):put 과 마찬가지로 commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			rowState = checkOrRecoverLock(tx, row, tableState, rowState);
		}
		rowState.addMutation(delete);
	}

	@Override
	public void delete(HaeinsaTransaction tx, List<HaeinsaDelete> deletes)
			throws IOException {
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
	public void commitSingleRowPutOnly(HaeinsaRowTransaction rowState, byte[] row) throws IOException{
		HaeinsaTransaction tx = rowState.getTableTransaction().getTransaction();
		Put put = new Put(row);
		HaeinsaPut haeinsaPut = (HaeinsaPut) rowState.getMutations().remove(0);
		for (HaeinsaKeyValue kv : Iterables.concat(haeinsaPut.getFamilyMap().values())){
			put.add(kv.getFamily(), kv.getQualifier(), tx.getCommitTimestamp(), kv.getValue());
		}
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, tx.getCommitTimestamp());
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getCommitTimestamp(), TRowLocks.serialize(newRowLock));
		
		byte[] currentRowLockBytes = TRowLocks.serialize(rowState.getCurrent());
		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
			throw new ConflictException("can't acquire row's lock, commitSingleRowPutOnly failed");
		}else {
			rowState.setCurrent(newRowLock);
		}
	}
	
	/** 
	 * Commit single row read only Transaction. 
	 * Read {@link TRowLock} from HBase and compare that lock with saved one which have retrieved when start transaction.
	 * If TRowLock is changed, it means transaction is failed, so throw {@link ConflictException}. 
	 * @param rowState
	 * @param row
	 * @throws IOException  ConflictException, HBase IOException.
	 */
	public void commitSingleRowReadOnly(HaeinsaRowTransaction rowState, byte[] row) throws IOException{
		checkSingleRowLock(rowState, row);
	}
	
	/**
	 * Read {@link TRowLock} from HBase and compare that lock with prevRowLock.
	 * If TRowLock is changed, it means transaction is failed, so throw {@link ConflictException}. 
	 * @param prevRowLock
	 * @param row
	 * @throws IOException ConflictException, HBase IOException.
	 * @throws NullPointException if oldLock is null (haven't read lock from HBase) 
	 */
	public void checkSingleRowLock(HaeinsaRowTransaction rowState, byte[] row) throws IOException{
		TRowLock currentRowLock = getRowLock(row);
		if (!rowState.getCurrent().equals(currentRowLock)){
			throw new ConflictException("this row is modified, commitSingleRowReadOnly failed");
		}
	}

	@Override
	public void prewrite(HaeinsaRowTransaction rowState, byte[] row, boolean isPrimary)
			throws IOException {
		Put put = new Put(row);
		Set<TCellKey> prewritten = Sets.newTreeSet();
		//	order of remaining as TRemove, TPut, TRemove, TPut, ...
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

		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION,
				TRowLockState.PREWRITTEN, tx.getCommitTimestamp())
				.setCurrentTimestmap(tx.getPrewriteTimestamp());
		if (isPrimary) {
			//	for primary row
			for(Entry<TRowKey, HaeinsaRowTransaction> rowStateEntry : tx.getMutationRowStates().entrySet()){
				TRowKey rowKey = rowStateEntry.getKey();
				if ((Bytes.equals(rowKey.getTableName(), getTableName()) 
						&& Bytes.equals(rowKey.getRow(), row))) {
					//	if this is primaryRow
					continue;
				}
				newRowLock.addToSecondaries(
						new TRowKey().setTableName(rowKey.getTableName()).setRow(rowKey.getRow()));
			}
		} else {
			//	for secondary rows
			newRowLock.setPrimary(tx.getPrimary());
		}

		newRowLock.setPrewritten(Lists.newArrayList(prewritten));
		newRowLock.setMutations(remaining);
		newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getCommitTimestamp(), TRowLocks.serialize(newRowLock));

		byte[] currentRowLockBytes = TRowLocks.serialize(rowState.getCurrent());

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			tx.abort();
			throw new ConflictException("can't acquire row's lock");
		} else {
			rowState.setCurrent(newRowLock);
		}

	}

	@Override
	public void applyMutations(HaeinsaRowTransaction rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrent().getMutationsSize() == 0) {
			//	해당 row 에 mutation 이 하나도 없을 때
			return;
		}

		List<TMutation> remaining = Lists.newArrayList(rowTxState.getCurrent().getMutations());
		long currentTimestamp = rowTxState.getCurrent().getCurrentTimestmap();
		
		for (int i = 0; i < remaining.size(); i++) {
			byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState.getCurrent());

			TMutation mutation = remaining.get(i);
			switch (mutation.getType()) {
			case PUT: {
				TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
				newRowLock.setCurrentTimestmap(currentTimestamp + i + 1);
				newRowLock.setMutations(remaining.subList(i + 1, remaining.size()));
				//	prewritten state 로 변하면서 ROW_LOCK_TIMEOUT 만큼 lock expiry 가 길어진다.  
				newRowLock.setExpiry(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
				Put put = new Put(row);
				put.add(LOCK_FAMILY, LOCK_QUALIFIER,
						newRowLock.getCurrentTimestmap(),
						TRowLocks.serialize(newRowLock));
				for (TKeyValue kv : mutation.getPut().getValues()) {
					put.add(kv.getKey().getFamily(),
							kv.getKey().getQualifier(),
							newRowLock.getCurrentTimestmap(), kv.getValue());
				}
				if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)) {
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
						delete.deleteFamily(removeFamily.array(), currentTimestamp + i + 1);
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

	@Override
	public void makeStable(HaeinsaRowTransaction rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState.getCurrent());
		HaeinsaTransaction transaction = rowTxState.getTableTransaction().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, commitTimestamp);
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

	@Override
	public void commitPrimary(HaeinsaRowTransaction rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState
				.getCurrent());
		HaeinsaTransaction transaction = rowTxState.getTableTransaction()
				.getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(TRowLockState.COMMITTED);
		//	make prewritten to null
		newRowLock.setPrewrittenIsSet(false);
		//	expiry 가 ROW_LOCK_TIMEOUT 만큼 뒤로 다시 밀린다. 
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
	public void abortPrimary(HaeinsaRowTransaction rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = TRowLocks.serialize(rowTxState
				.getCurrent());
		HaeinsaTransaction transaction = rowTxState.getTableTransaction()
				.getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = rowTxState.getCurrent().deepCopy();
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(TRowLockState.ABORTED);
		newRowLock.setMutationsIsSet(false);
		//	이 expiry extension 때문에 이미 abort 된 primary row 가 다시 두 개 이상의 Client 에 의해서 abort 가 될 때 
		//	하나의 Client 만 lock 을 가져오는 데 성공한다. 
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

	@Override
	public void deletePrewritten(HaeinsaRowTransaction rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrent().getPrewrittenSize() == 0) {
			//	nothing to do
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
	
	
	/**
	 * Transaction 없이 Scan 할 때 사용하는 {@link HaeinsaResultScanner}.
	 * @author Youngmok Kim
	 *
	 */
	private class SimpleClientScanner implements HaeinsaResultScanner {
		private final ResultScanner scanner;
		
		public SimpleClientScanner(ResultScanner scanner){
			this.scanner = scanner;
		}

		@Override
		public Iterator<HaeinsaResult> iterator() {
			return Iterators.transform(scanner.iterator(), new Function<Result, HaeinsaResult>(){
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
			for (int i=0;i<resultArray.length;i++){
				transformed[i] = new HaeinsaResult(resultArray[i]);
			}
			return transformed;
		}
		
		@Override
		public HaeinsaResult next() throws IOException {
			Result result = scanner.next();
			if (result != null){
				return new HaeinsaResult(result);
			}else{
				return null;
			}
		}
		
		@Override
		public void close() {
			scanner.close();
		}
	}

	/**
	 * {@link HaeinsaGet} 와 {@link HaeinsaScan}, {@link HaeinsaIntraScan} 이 Transaction 동안 put/delete 된 데이터를 정확히 읽게 하기 위해서 
	 * 을 위해서 Put/Delete Projection 을 해주는 class 이다. 
	 * 
	 * <p>T = { W1(x), R2(X), R3(Y), W4(X) } 와 같은 transaction 을 생각해 보자. 
	 * Haeinsa 에서는 Transaction 이 commit 되기 전에는 HBase 에 prewrite 이 일어나지 않는다. 
	 * 따라서 R(X) 가 HBase 에서만 데이터를 읽게 되면 W1(x) 에서 쓴 데이터를 R2(x) 에서 읽지 못하게 된다. 
	 * 이를 방지하기 위해서 W1(x) 에서 쓰인 데이터를 Client 쪽에서 모아서 R2(x) 작업이 일어날 때 projection 을 해줄 필요가 있다.
	 * <p> {@link ClientScanner} 는 이 projection 작업을 위한 class 로, 하나의 {@link HaeinsaTable} 에 대한 Scanner 들만 모아 놓게 된다. 
	 * @author Myungbo Kim
	 *
	 */
	private class ClientScanner implements HaeinsaResultScanner { 
		private final HaeinsaTransaction tx;
		private final HaeinsaTableTransaction tableState;
		private boolean initialized = false;
		private final NavigableSet<HaeinsaKeyValueScanner> scanners = Sets
				.newTreeSet(HaeinsaKeyValueScanner.COMPARATOR);
		private final List<HaeinsaKeyValueScanner> scannerList = Lists
				.newArrayList();
		//	tracking delete of one specific row.
		private final HaeinsaDeleteTracker deleteTracker = new HaeinsaDeleteTracker();
		private final HaeinsaColumnTracker columnTracker;
		/**
		 * true if TRowLock inside scanners, 
		 * false if TRowLock is already included inside tableState.getRowStates().get(row) 
		 */
		private final boolean lockInclusive;
		/**
		 * -1 if not used. ( Get / Scan ) 
		 */
		private final int batch;
		private final Map<byte[], NavigableSet<byte[]>> familyMap; 
		private HaeinsaKeyValue prevKV = null;
		private long maxSeqID = Long.MAX_VALUE;
		
		/**
		 * 
		 * @param tx
		 * @param scanners
		 * @param familyMap
		 * @param lockInclusive 
		 * 						- scanners 안에 TRowLock 에 관한 정보가 포함되어 있는지를 나타낸다. 만약 포함되어 있지 않다면 
		 * 						{@link RowTransaction} 에 이미 저장되어 있는 정보를 가지고 오거나
		 * 						HBase 에서 직접 읽어야 한다. 
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
		 * @param intraScan
		 * 						- To support to use {@link ColumnRangeFilter}  
		 * @param lockInclusive 
		 * 						- scanners 안에 TRowLock 에 관한 정보가 포함되어 있는지를 나타낸다. 만약 포함되어 있지 않다면 
		 * 						{@link RowTransaction} 에 이미 저장되어 있는 정보를 가지고 오거나
		 * 						HBase 에서 직접 읽어야 한다. 
		 * 						
		 */
		public ClientScanner(HaeinsaTransaction tx,	Iterable<HaeinsaKeyValueScanner> scanners,
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

		/**
		 * {@link #scannerList} 로부터 {@link #scanners} 를 만들어 내는 함수이다.
		 * 하나의 ClientScanner 당 한 번씩만 불릴 수 있다.
		 * scannerList 와 scanners 를 따로 관리하는 이유는 {@link #nextScanner()} 가 scanners 에 들어 있는
		 * HaeinsaKeyValueScanner 를 하나씩 제거하는 방식으로 동작하기 때문이다.
		 * {@link #close()} 함수를 위해서는 ClientScanner 가 만들어 질 때 있었던 
		 * 모든 HeainsaKeyValueScanner 들에 관한 정보가 있어야 하는데, 
		 * @throws IOException
		 */
		private void initialize() throws IOException {
			try {
				for (HaeinsaKeyValueScanner scanner : scannerList){
					HaeinsaKeyValue peeked = scanner.peek();
					if (peeked != null){
						scanners.add(scanner);
					}
				}
				initialized = true;
			} catch (Exception e) {
				throw new IOException(e.getMessage(), e);
			}
		}

		/**
		 * ClientScanner 에 대한 iterator 를 return 한다. 내부적으로 {@link #next()} 을 사용하도록 구현되어 있다.
		 */
		@Override
		public Iterator<HaeinsaResult> iterator() {
			return new Iterator<HaeinsaResult>() {
				//	if current is null, whether scan is not started or next() was called.
				//	if hasNext() is called, next data will be ready on current.
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
						//	IOException 을 throw 할 수 없는 hasNext() 의 특성상 RunTimeException 으로
						//	감싸서 throw 함.
						throw new IllegalStateException(e.getMessage(), e);
					}
					return false;
				}
			};
		}
		
		/**
		 * 정해진 row 에 대한 TRowLock 이 있는지 {@link #scanners} 중에서 찾아서 return 한다. 없으면 null 을 return 한다.
		 * 만약 {@link #scanners} 중 하나가 row 보다 선행하는 {@link HaeinsaKeyValueScanner} 라면 
		 * 해당 Scanner 는 이 method 에 의해서 선택되지 않는다.
		 * 즉, 올바른 동작을 위해서는 {@link #scanners} 에 들어 있는 모든 HaeinsaKeyValueScanner 의 peek() 값이 
		 * 이 method 에 argument 로 주어지는 row 보다 크거나 같은 row 값을 가지고 있어야 한다.  
		 * @param row
		 * @return null if there is no TRowLock information inside scanners, return rowLock otherwise.
		 * @throws IOException
		 */
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
				//	move scannerList -> scanners
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
					if (lockInclusive){
						//	HBaseScanScanner 나 HBaseGetScanner 에 의해서 넘어온 HaeinsaKeyValue 중에
						//	TRowLock 정보가 이미 존재함을 의미한다.
						TRowLock currentRowLock = peekLock(currentKV.getRow());
						HaeinsaRowTransaction rowState = tableState.createOrGetRowState(currentKV.getRow());
						if (rowState.getCurrent() == null){
							//	rowState 가 createOrGetRowState 에 의해서 방금 만들어졌다는 뜻이다.
							//	따라서 적당한 TRowLock 값을 찾아서 rowState 에 넣어줘야 한다.
							if (currentRowLock == null){
								//	HBase 에 TRowLock 정보가 없는 경우
								//	이 것은 Haeinsa 가 HBase 로부터의 lazy-migration 을 지원해야 하기 때문에 생기는 case 이다.  
								//	
								//	TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, Long.MIN_VALUE) 를 HaeinsaRowTransaction에 넣으면 된다.
								//	이 TRowLock 값은 후에 commit 시에 적당한 초기 TRowLock 으로 override 되어 HBase 에 기록된다. 
								currentRowLock = TRowLocks.deserialize(null);
								rowState.setCurrent(currentRowLock);
							}
							
							if (checkAndIsShouldRecover(currentRowLock)){
								//	when currentRowLock is not stable but expired.
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
								//	when currentRowLock is stable 
								rowState.setCurrent(currentRowLock);
							}
						}
						else{
							//	rowState 가 이미 있다는 뜻으로, scan 이나 get 으로 읽어온 TRowLock 이 아니라 
							//	rowState 에 포함된 current 을 사용하게 된다.
						}
						//	여기까지 왔다는 것은 currentKV.getRow() 에 대한 TRowLock 이 RowState 에 저장되어 있다는 뜻이다. 
					}
					prevKV = currentKV;
				}
				
				if (Bytes.equals(prevKV.getRow(), currentKV.getRow())) {
					if (currentScanner.getSequenceID() > maxSeqID) {
						//	too old data, ignore
					} else if (Bytes.equals(currentKV.getFamily(), LOCK_FAMILY) 
							&& Bytes.equals(currentKV.getQualifier(), LOCK_QUALIFIER)){
						//	if currentKV is Lock, ignore
					} else if (currentKV.getType() == Type.DeleteColumn || currentKV.getType() == Type.DeleteFamily){
						//	if currentKV is delete
						deleteTracker.add(currentKV, currentScanner.getSequenceID());
					} else if (prevKV == currentKV
							|| !(Bytes.equals(prevKV.getRow(), currentKV.getRow())
									&& Bytes.equals(prevKV.getFamily(), currentKV.getFamily()) 
									&& Bytes.equals(prevKV.getQualifier(), currentKV.getQualifier()))) {
						//	prevKV 와 currentKV 의 reference 가 같은 경우는 해당 currentKV 가 새로운 row 인 경우이다. 
						// 그 외에 Row, Family, Qualifier 모두가 같은 경우가 더 나오면 무시한다. ( 나올 가능성은 거의 없다. )
						if (!deleteTracker.isDeleted(currentKV, currentScanner.getSequenceID()) 
								&& columnTracker.isMatched(currentKV)){
							//	if currentKV is not deleted and inside scan range
							sortedKVs.add(currentKV);
							prevKV = currentKV;
						}
					}
					
					nextScanner(currentScanner);
				} else {
					//	currentKV is different row with prevKV, so reset deleteTracker & maxSeqID
					deleteTracker.reset();
					prevKV = null;
					maxSeqID = Long.MAX_VALUE;
					if (sortedKVs.size() > 0){
						//	currentKV 가 다음 row 로 넘어 갔고 그 전 row 에서 하나 이상의 KV 가 scan 조건을 만족시킨 경우,
						//	scan 과정을 멈추고 현재까지의 모인 HaeinsaKeyValue를 묶어서 HaeinsaResult 를 반환해야 한다.
						break;
					}
				}
				if (batch > 0 && sortedKVs.size() >= batch){
					//	if intraScan & sortedKVs have more elements than batch.
					break;
				}
			}
			if (sortedKVs.size() > 0) {
				return new HaeinsaResult(sortedKVs);
			} else {
				//	scanners are exhausted.
				return null;
			}
		}

		/**
		 * Moving index of scanner of currentScanner by one.
		 * If there is no more element at the scanner, remove currentScanner from scanners ( NavigableSet ).
		 * @param currentScanner
		 * @throws IOException
		 */
		private void nextScanner(HaeinsaKeyValueScanner currentScanner)
				throws IOException {
			scanners.remove(currentScanner);
			currentScanner.next();
			HaeinsaKeyValue currentScannerNext = currentScanner.peek();
			if (currentScannerNext != null) {
				//	if currentScanner is not exhausted.
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
	 * Scan 으로 받은 {@link ResultScanner} 를 wrapping 해서 그 내부에 들어 있는 KeyValue 들을 {@link HaeinsaKeyValue} 로 
	 * 감싼 다음에 Scanner interface 로 접근할 수 있게 해주는 Class 이다.
	 * {@link ResultScanner} class 처럼 HBaseScanScanner 는 단일 row 에 대한 정보를 담고 있을 수도 있고, 여러 row 에 대한 
	 * 정보를 담고 있을 수도 있다. HBaseScanScanner 는 {@link HaeinsaScan} 과 {@link HaeinsaIntraScan} 의 두 가지 경우에 사용된다.
	 * <p>Scan 을 통해서 읽어오는 row 에서 {@link TRowLock} 이 {@link TRowLockState#STABLE} 이 아닐 경우에는 
	 * {@link HBaseGetScanner} 를 사용해서 다시 읽어와서 더 낮은 sequenceID 로 override 하기 때문에 
	 * HBaseScanScanner 는 sequenceID control 이 필요하지 않다. 
	 * 따라서 {@link #getSequenceID()} 는 언제나 Long.MAX_VALUE 를 return 한다.
	 * @author Myungbo Kim
	 *
	 */
	private static class HBaseScanScanner implements
			HaeinsaKeyValueScanner {
		private final ResultScanner resultScanner;
		/**
		 * current is null when scan is not started or next() is called last time.
		 */
		private HaeinsaKeyValue current;
		/**
		 * currentResult is null when there is no more elements to scan in resultScanner or scan is not started.
		 * currentResult 는 단일 row 에 대한 정보만 가지고 있다. 
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
				if (currentResult == null
						|| (currentResult != null && resultIndex >= currentResult.size())) {
					currentResult = resultScanner.next();
					if (currentResult != null && currentResult.isEmpty()) {
						currentResult = null;
					}
					resultIndex = 0;
				}
				if (currentResult == null) {
					//	if currentResult is still null at this point, that means there is no more KV to scan. 
					return null;
				}
				//	First scan or next() was called last time so move resultIndex.
				current = new HaeinsaKeyValue(currentResult.raw()[resultIndex]);
				resultIndex++;

				return current;
			} catch (IOException e) {
				//	TODO
				//	IOException ( 실제로는 next() 에 대한 ConflictException ) 을 감싸서 RuntimeException 으로
				//	올리는 것보다 더 좋은 방법이 있는 지 고민해 봅시다.
				//	이 function 이 HaeinsaKeyValue 의 Comparator 로 들어가 버리기 때문에 
				//	compare 연산에서 RuntimeException 이 throw 될 수 있다.
				//
				//	현재와 같은 상태를 유지하고 싶으면 Transaction 자체가 IOException 이 아니라 Exception 을 던지도록 해서
				//	사용자가 IOException 뿐 아니라 RuntimeException 도 catch 하도록 강제하는 방법이 있다.
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
			//	Scan 은 언제나 HBase 에 실제로 저장되어 있는 데이터를 가져온 것이기 때문에 sequenceID 가 가장 크다.
			//	( 가장 오래된 데이터이다. ) 
			return Long.MAX_VALUE;
		}

		@Override
		public void close() {
			resultScanner.close();
		}
	}

	/**
	 * Get 으로 받은 {@link Result} 를 wrapping 해서 그 내부에 들어 있는 KeyValue 들을 {@link HaeinsaKeyValue} 로 감싼 다음에 
	 * Scanner interface 로 접근할 수 있게 해주는 Class 이다.
	 * {@link Result} class 처럼 HBaseGetScanner 도 단일 row 에 대한 정보만을 포함하고 있다.
	 * 따라서 {@link #peek}이나 {@link #next}를 통해서 접근하는 모든 HaeinsaKeyValue 는 같은 row 를 가지게 된다.
	 * <p>HBaseGetScanner 는 현재 2가지 경우에 사용되는데, 첫 번째는 {@link HaeinsaTable#get} 내부에서 사용되는 경우이며 
	 * 이 경우에는 항상 Long.MAX_VALUE 의 sequenceID 를 가지게 된다. 두 번째는 {@link ClientScanner} 내부에서 사용되는 경우이다.
	 * 이 때는 Lock 을 Recover 한 후에 해당 값을 다시 읽어 오는데 사용하게 된다. 
	 * 따라서 {@link #sequenceID} 는 ClientScanner 에서 지금까지 사용했던 sequenceId 보다 작은 값이어야 한다. ( 더 최근의 값이므로 )
	 * @author Myungbo Kim
	 *
	 */
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
				//	null if result is empty
				this.result = null;
			}
			//	bigger sequenceID is older one.
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
