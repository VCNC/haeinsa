package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.Constants.LOCK_FAMILY;
import static kr.co.vcnc.haeinsa.Constants.LOCK_QUALIFIER;
import static kr.co.vcnc.haeinsa.Constants.ROW_LOCK_TIMEOUT;
import static kr.co.vcnc.haeinsa.Constants.ROW_LOCK_VERSION;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import kr.co.vcnc.haeinsa.thrift.CellKey;
import kr.co.vcnc.haeinsa.thrift.HaeinsaThriftUtils;
import kr.co.vcnc.haeinsa.thrift.RowKey;
import kr.co.vcnc.haeinsa.thrift.RowLock;
import kr.co.vcnc.haeinsa.thrift.RowState;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.jersey.api.ConflictException;

class TableImpl implements Table.PrivateIface {
	
	private final HTableInterface table;
	
	public TableImpl(HTableInterface table){
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
	public Result get(Transaction tx, Get get) throws IOException {
		byte[] row = get.getRow();
		TableTransactionState tableState = tx.createOrGetTableState(this.table.getTableName());
		RowTransactionState rowState = tableState.getRowStates().get(row);
		
		if (rowState == null){ 
			get.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
		}
		
		Result result = table.get(get);
		
		if (rowState == null){
			rowState = tableState.createOrGetRowState(row);
			byte[] currentRowLockBytes = result.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
			RowLock currentRowLock = HaeinsaThriftUtils.deserialize(currentRowLockBytes);
			
			if (currentRowLock.getState() != RowState.STABLE){
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
	public Result[] get(Transaction tx, List<Get> gets) throws IOException {
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
	public void put(Transaction tx, Put put) throws IOException {
		byte[] row = put.getRow();
		TableTransactionState tableState = tx.createOrGetTableState(this.table.getTableName());
		RowTransactionState rowState = tableState.getRowStates().get(row);
		if (rowState == null){
			// TODO commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			RowLock rowLock = getRowLock(row);
			if (rowLock.getState() != RowState.STABLE){
				throw new ConflictException("this row is unstable.");
			}
			// TODO 현재 lock의 상태가 STABLE이 아니고 timeout이 지났으면, recover 해야 한다.
			rowState = tableState.createOrGetRowState(row);
			rowState.setCurrentRowLock(rowLock);
			rowState.setOriginalRowLock(rowLock);
		}
		rowState.getMutations().add(put);
	}

	@Override
	public void put(Transaction tx, List<Put> puts) throws IOException {
		for (Put put : puts){
			put(tx, put);
		}
	}

	@Override
	public void delete(Transaction tx, Delete delete) throws IOException {
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
			RowLock rowLock = getRowLock(row);
			if (rowLock.getState() != RowState.STABLE){
				throw new ConflictException("this row is unstable.");
			}
			// TODO 현재 lock의 상태가 STABLE이 아니고 timeout이 지났으면, recover 해야 한다.
			rowState = tableState.createOrGetRowState(row);
			rowState.setCurrentRowLock(rowLock);
			rowState.setOriginalRowLock(rowLock);
		}
		rowState.getMutations().add(delete);
	}

	@Override
	public void delete(Transaction tx, List<Delete> deletes) throws IOException {
		for (Delete delete : deletes){
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
		Set<CellKey> puts = Sets.newTreeSet();
		Set<CellKey> deletes = Sets.newTreeSet();
		Transaction tx = rowState.getTableTxState().getTransaction();
		for (Mutation mutation : rowState.getMutations()){
			if (mutation instanceof Put){
				for (Entry<byte[], List<KeyValue>> familyEntry : mutation.getFamilyMap().entrySet()){
					for (KeyValue kv : familyEntry.getValue()){
						put.add(familyEntry.getKey(), kv.getQualifier(), tx.getCommitTimestamp(), kv.getValue());
						CellKey cellKey = new CellKey();
						cellKey.setFamily(familyEntry.getKey());
						cellKey.setQualifier(kv.getQualifier());
						deletes.remove(cellKey);
						puts.add(cellKey);
					}
				}
			} else if (mutation instanceof Delete) {
				for (Entry<byte[], List<KeyValue>> familyEntry : mutation.getFamilyMap().entrySet()){
					for (KeyValue kv : familyEntry.getValue()){
						CellKey cellKey = new CellKey();
						cellKey.setFamily(familyEntry.getKey());
						cellKey.setQualifier(kv.getQualifier());
						deletes.add(cellKey);
					}
				}
			}
		}
		RowLock newRowLock = new RowLock(ROW_LOCK_VERSION, RowState.PREWRITTEN, tx.getCommitTimestamp());
		if (isPrimary){
			for (Entry<byte[], TableTransactionState> tableStateEntry : tx.getTableStates().entrySet()){
				for (Entry<byte[], RowTransactionState> rowStateEntry : tableStateEntry.getValue().getRowStates().entrySet()){
					if ((Bytes.equals(tableStateEntry.getKey(), getTableName()) && Bytes.equals(rowStateEntry.getKey(), row))){
						continue;
					}
					newRowLock.addToSecondaries(new RowKey().setTableName(tableStateEntry.getKey()).setRow(rowStateEntry.getKey()));
				}
			}
		}else{
			newRowLock.setPrimary(tx.getPrimary());
		}
		
		newRowLock.setPuts(Lists.newArrayList(puts));
		newRowLock.setDeletes(Lists.newArrayList(deletes));
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
		if (rowTxState.getCurrentRowLock().getDeletesSize() == 0){
			return;
		}
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		Transaction transaction = rowTxState.getTableTxState().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		Delete delete = new Delete(row);
		for (CellKey cellKey : rowTxState.getCurrentRowLock().getDeletes()){
			delete.deleteColumns(cellKey.getFamily(), cellKey.getQualifier(), commitTimestamp);
		}
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
		RowLock newRowLock = new RowLock(ROW_LOCK_VERSION, RowState.STABLE, commitTimestamp);
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
		RowLock newRowLock = new RowLock(rowTxState.getCurrentRowLock());
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(RowState.COMMITTED);
		newRowLock.setPutsIsSet(false);
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
	public RowLock getRowLock(byte[] row) throws IOException {
		org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(row);
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
		RowLock newRowLock = new RowLock(rowTxState.getCurrentRowLock());
		newRowLock.setCommitTimestamp(commitTimestamp);
		newRowLock.setState(RowState.ABORTED);
		newRowLock.setDeletesIsSet(false);
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
		if (rowTxState.getCurrentRowLock().getPutsSize() == 0){
			return;
		}
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		Transaction transaction = rowTxState.getTableTxState().getTransaction();
		long commitTimestamp = transaction.getCommitTimestamp();
		Delete delete = new Delete(row);
		for (CellKey cellKey : rowTxState.getCurrentRowLock().getPuts()){
			delete.deleteColumn(cellKey.getFamily(), cellKey.getQualifier(), commitTimestamp);
		}
		if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, delete)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}		
	}	
}
