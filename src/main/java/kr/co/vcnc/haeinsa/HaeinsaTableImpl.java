package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_FAMILY;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_QUALIFIER;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_TIMEOUT;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_VERSION;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import kr.co.vcnc.haeinsa.thrift.HaeinsaThriftUtils;
import kr.co.vcnc.haeinsa.thrift.generated.TCellKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.jersey.api.ConflictException;

class HaeinsaTableImpl implements HaeinsaTable.PrivateIface {
	
	private final HTableInterface table;
	
	public HaeinsaTableImpl(HTableInterface table){
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
	public Result get(Transaction tx, HaeinsaGet get) throws IOException {
		byte[] row = get.getRow();
		TableTransactionState tableState = tx.createOrGetTableState(this.table.getTableName());
		RowTransactionState rowState = tableState.getRowStates().get(row);
		
		Get hGet = new Get(get.getRow());
		hGet.getFamilyMap().putAll(get.getFamilyMap());
		if (rowState == null){ 
			get.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
		}
		
		Result result = table.get(hGet);
		
		if (rowState == null){
			rowState = tableState.createOrGetRowState(row);
			byte[] currentRowLockBytes = result.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
			TRowLock currentRowLock = HaeinsaThriftUtils.deserialize(currentRowLockBytes);
			
			if (currentRowLock.getState() != TRowLockState.STABLE){
				throw new ConflictException("this row is unstable.");
			}
			// TODO 현재 lock의 상태가 STABLE이 아니고 timeout이 지났으면, recover 해야 한다.
			rowState.setOriginalRowLock(currentRowLock);
			rowState.setCurrentRowLock(currentRowLock);
			if (!result.isEmpty()){
				List<KeyValue> kvs = Lists.newArrayList();
				for (KeyValue kv : result.list()){
					if (!(Bytes.equals(kv.getFamily(), LOCK_FAMILY) 
							&& Bytes.equals(kv.getQualifier(), LOCK_QUALIFIER))){
						kvs.add(kv);
					}
				}
				result = new Result(kvs);
			}
		}

		return result;
	}

	@Override
	public Result[] get(Transaction tx, List<HaeinsaGet> gets) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public ResultScanner getScanner(Transaction tx, Scan scan)
			throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public ResultScanner getScanner(Transaction tx, byte[] family)
			throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public ResultScanner getScanner(Transaction tx, byte[] family,
			byte[] qualifier) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void put(Transaction tx, HaeinsaPut put) throws IOException {
		byte[] row = put.getRow();
		TableTransactionState tableState = tx.createOrGetTableState(this.table.getTableName());
		RowTransactionState rowState = tableState.getRowStates().get(row);
		if (rowState == null){
			// TODO commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			TRowLock rowLock = getRowLock(row);
			if (rowLock.getState() != TRowLockState.STABLE){
				throw new ConflictException("this row is unstable.");
			}
			// TODO 현재 lock의 상태가 STABLE이 아니고 timeout이 지났으면, recover 해야 한다.
			rowState = tableState.createOrGetRowState(row);
			rowState.setCurrentRowLock(rowLock);
			rowState.setOriginalRowLock(rowLock);
		}
		rowState.addMutation(put);
	}

	@Override
	public void put(Transaction tx, List<HaeinsaPut> puts) throws IOException {
		for (HaeinsaPut put : puts){
			put(tx, put);
		}
	}

	@Override
	public void delete(Transaction tx, HaeinsaDelete delete) throws IOException {
		// 삭제는 특정 Cell만 삭제 가능하다. 
		// TODO Family도 삭제 가능하게 만들어야 함. 
		byte[] row = delete.getRow();
		Preconditions.checkArgument(delete.getFamilyMap().size() <= 0, "can't delete an entire row.");
		for (Entry<byte[], List<KeyValue>> familyEntry : delete.getFamilyMap().entrySet()){
			Preconditions.checkArgument(familyEntry.getValue() == null, "can't delete a row's family.");
			for (KeyValue kv : familyEntry.getValue()){
				Preconditions.checkArgument(kv.getType() == KeyValue.Type.Delete.getCode(), "can't delete a cell at specific timestamp.");
			}
		}
		TableTransactionState tableState = tx.createOrGetTableState(this.table.getTableName());
		RowTransactionState rowState = tableState.getRowStates().get(row);
		if (rowState == null){
			// TODO commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			TRowLock rowLock = getRowLock(row);
			if (rowLock.getState() != TRowLockState.STABLE){
				throw new ConflictException("this row is unstable.");
			}
			// TODO 현재 lock의 상태가 STABLE이 아니고 timeout이 지났으면, recover 해야 한다.
			rowState = tableState.createOrGetRowState(row);
			rowState.setCurrentRowLock(rowLock);
			rowState.setOriginalRowLock(rowLock);
		}
		rowState.addMutation(delete);
	}

	@Override
	public void delete(Transaction tx, List<HaeinsaDelete> deletes) throws IOException {
		for (HaeinsaDelete delete : deletes){
			delete(tx, delete);
		}
	}

	@Override
	public void close() throws IOException {
		table.close();
	}

	@Override
	public void prewrite(RowTransactionState rowState, byte[] row, boolean isPrimary) throws IOException {
		Put put = new Put(row);
		Set<TCellKey> puts = Sets.newTreeSet();
		Set<TCellKey> deletes = Sets.newTreeSet();
		Transaction tx = rowState.getTableTxState().getTransaction();
		for (HaeinsaMutation mutation : rowState.getMutations()){
			if (mutation instanceof HaeinsaPut){
				for (Entry<byte[], List<KeyValue>> familyEntry : mutation.getFamilyMap().entrySet()){
					for (KeyValue kv : familyEntry.getValue()){
						put.add(familyEntry.getKey(), kv.getQualifier(), tx.getCommitTimestamp(), kv.getValue());
						TCellKey cellKey = new TCellKey();
						cellKey.setFamily(familyEntry.getKey());
						cellKey.setQualifier(kv.getQualifier());
						deletes.remove(cellKey);
						puts.add(cellKey);
					}
				}
			} else if (mutation instanceof HaeinsaDelete) {
				for (Entry<byte[], List<KeyValue>> familyEntry : mutation.getFamilyMap().entrySet()){
					for (KeyValue kv : familyEntry.getValue()){
						TCellKey cellKey = new TCellKey();
						cellKey.setFamily(familyEntry.getKey());
						cellKey.setQualifier(kv.getQualifier());
						deletes.add(cellKey);
					}
				}
			}
		}
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.PREWRITTEN, tx.getCommitTimestamp());
		if (isPrimary){
			for (Entry<byte[], TableTransactionState> tableStateEntry : tx.getTableStates().entrySet()){
				for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
					if ((Bytes.equals(tableStateEntry.getKey(), getTableName()) && Bytes.equals(rowStateEntry.getKey(), row))){
						continue;
					}
					newRowLock.addToSecondaries(new TRowKey().setTableName(tableStateEntry.getKey()).setRow(rowStateEntry.getKey()));
				}
			}
		}else{
			newRowLock.setPrimary(tx.getPrimary());
		}
		
		newRowLock.setPrewritten(Lists.newArrayList(puts));
		
//		newRowLock.setDeletes(Lists.newArrayList(deletes));
		newRowLock.setTimeout(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getCommitTimestamp(), HaeinsaThriftUtils.serialize(newRowLock));
		
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowState.getCurrentRowLock());
		
		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}else{
			rowState.setCurrentRowLock(newRowLock);
		}
		
	}

	@Override
	public void applyDeletes(RowTransactionState rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrentRowLock().getMutationsSize() == 0){
			return;
		}
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		Transaction transaction = rowTxState.getTableTxState().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		Delete delete = new Delete(row);
//		for (TCellKey cellKey : rowTxState.getCurrentRowLock().getDeletes()){
//			delete.deleteColumns(cellKey.getFamily(), cellKey.getQualifier(), commitTimestamp);
//		}
		if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, delete)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}
	}

	@Override
	public void makeStable(RowTransactionState rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		Transaction transaction = rowTxState.getTableTxState().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, commitTimestamp);
		byte[] newRowLockBytes = HaeinsaThriftUtils.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, commitTimestamp, newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
			// 실패하는 경우는 다른 쪽에서 먼저 commit을 한 경우이므로 오류 없이 넘어가면 된다.
		}else{
			rowTxState.setCurrentRowLock(newRowLock);
		}
	}

	@Override
	public void commitPrimary(RowTransactionState rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		Transaction transaction = rowTxState.getTableTxState().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = new TRowLock(rowTxState.getCurrentRowLock());
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(TRowLockState.COMMITTED);
		newRowLock.setPrewrittenIsSet(false);
		newRowLock.setTimeout(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
		
		byte[] newRowLockBytes = HaeinsaThriftUtils.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, commitTimestamp, newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}else{
			rowTxState.setCurrentRowLock(newRowLock);
		}
	}

	@Override
	public TRowLock getRowLock(byte[] row) throws IOException {
		Get get = new Get(row);
		get.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
		Result result = table.get(get);
		if (result.isEmpty()){
			return null;
		}else{
			byte[] rowLockBytes = result.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
			return HaeinsaThriftUtils.deserialize(rowLockBytes);
		}
	}

	@Override
	public void abortPrimary(RowTransactionState rowTxState, byte[] row)
			throws IOException {
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		Transaction transaction = rowTxState.getTableTxState().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		TRowLock newRowLock = new TRowLock(rowTxState.getCurrentRowLock());
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(TRowLockState.ABORTED);
//		newRowLock.setDeletesIsSet(false);
		newRowLock.setTimeout(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
		
		byte[] newRowLockBytes = HaeinsaThriftUtils.serialize(newRowLock);
		Put put = new Put(row);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, commitTimestamp, newRowLockBytes);

		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}else{
			rowTxState.setCurrentRowLock(newRowLock);
		}
	}
	
	@Override
	public void deletePuts(RowTransactionState rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrentRowLock().getPrewrittenSize() == 0){
			return;
		}
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		Transaction transaction = rowTxState.getTableTxState().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		Delete delete = new Delete(row);
		for (TCellKey cellKey : rowTxState.getCurrentRowLock().getPrewritten()){
			delete.deleteColumn(cellKey.getFamily(), cellKey.getQualifier(), commitTimestamp);
		}
		if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, delete)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}		
	}	
}
