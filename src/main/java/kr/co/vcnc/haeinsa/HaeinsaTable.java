package kr.co.vcnc.haeinsa;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_FAMILY;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.LOCK_QUALIFIER;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_TIMEOUT;
import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.HaeinsaThriftUtils;
import kr.co.vcnc.haeinsa.thrift.generated.TCellKey;
import kr.co.vcnc.haeinsa.thrift.generated.TKeyValue;
import kr.co.vcnc.haeinsa.thrift.generated.TMutation;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class HaeinsaTable implements HaeinsaTableInterface.Private {
	
	private final HTableInterface table;
	
	public HaeinsaTable(HTableInterface table){
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
		if (rowState == null && hGet.hasFamilies()){ 
			hGet.addColumn(LOCK_FAMILY, LOCK_QUALIFIER);
		}
		
		Result result = table.get(hGet);
		
		if (rowState == null){
			rowState = tableState.createOrGetRowState(row);
			byte[] currentRowLockBytes = result.getValue(LOCK_FAMILY, LOCK_QUALIFIER);
			TRowLock currentRowLock = HaeinsaThriftUtils.deserialize(currentRowLockBytes);
			
			if (checkAndIsShouldRecover(currentRowLock)){
				recover(tx, row, currentRowLock);
			}
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
	
	private boolean checkAndIsShouldRecover(TRowLock rowLock) throws IOException{
		if (rowLock.getState() != TRowLockState.STABLE){
			if (rowLock.isSetTimeout() && rowLock.getTimeout() > System.currentTimeMillis()){
				return true;
			}
			throw new ConflictException("this row is unstable.");
		}
		return false;
	}
	
	private void recover(Transaction tx, byte[] row, TRowLock rowLock) throws IOException {
		Transaction previousTx = tx.getManager().getTransaction(getTableName(), row);
		previousTx.recover();
	}

	@Override
	public void put(Transaction tx, HaeinsaPut put) throws IOException {
		byte[] row = put.getRow();
		TableTransactionState tableState = tx.createOrGetTableState(this.table.getTableName());
		RowTransactionState rowState = tableState.getRowStates().get(row);
		if (rowState == null){
			// TODO commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			TRowLock rowLock = getRowLock(row);
			if (checkAndIsShouldRecover(rowLock)){
				recover(tx, row, rowLock);
			}
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
		byte[] row = delete.getRow();
		// 전체 Row의 삭제는 불가능하다.
		Preconditions.checkArgument(delete.getFamilyMap().size() <= 0, "can't delete an entire row.");
		TableTransactionState tableState = tx.createOrGetTableState(this.table.getTableName());
		RowTransactionState rowState = tableState.getRowStates().get(row);
		if (rowState == null){
			// TODO commit 시점에 lock을 가져오도록 바꾸는 것도 고민해봐야 함.
			TRowLock rowLock = getRowLock(row);
			if (checkAndIsShouldRecover(rowLock)){
				recover(tx, row, rowLock);
			}
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
		Set<TCellKey> prewritten = Sets.newTreeSet();
		List<TMutation> remaining = Lists.newArrayList();
		Transaction tx = rowState.getTableTxState().getTransaction();
		if (rowState.getMutations().size() > 0){
			if (rowState.getMutations().get(0) instanceof HaeinsaPut){
				HaeinsaPut haeinsaPut = (HaeinsaPut) rowState.getMutations().remove(0);
				for (KeyValue kv : Iterables.concat(haeinsaPut.getFamilyMap().values())){
					put.add(kv.getFamily(), kv.getQualifier(), tx.getPrewriteTimestamp(), kv.getValue());
					TCellKey cellKey = new TCellKey();
					cellKey.setFamily(kv.getFamily());
					cellKey.setQualifier(kv.getQualifier());
					prewritten.add(cellKey);
				}
			}
			for (HaeinsaMutation mutation : rowState.getMutations()){
				remaining.add(mutation.toTMutation());
			}
		}
		
		TRowLock newRowLock = new TRowLock(ROW_LOCK_VERSION, TRowLockState.PREWRITTEN, tx.getCommitTimestamp()).setCurrentTimestmap(tx.getPrewriteTimestamp());
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
		
		newRowLock.setPrewritten(Lists.newArrayList(prewritten));
		newRowLock.setMutations(remaining);
		newRowLock.setTimeout(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
		put.add(LOCK_FAMILY, LOCK_QUALIFIER, tx.getCommitTimestamp(), HaeinsaThriftUtils.serialize(newRowLock));
		
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowState.getCurrentRowLock());
		
		if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			tx.abort();
			throw new ConflictException("can't acquire row's lock");
		}else{
			rowState.setCurrentRowLock(newRowLock);
		}
		
	}

	@Override
	public void applyMutations(RowTransactionState rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrentRowLock().getMutationsSize() == 0){
			return;
		}
		
		List<TMutation> remaining = Lists.newArrayList(rowTxState.getCurrentRowLock().getMutations());
		long currentTimestamp = rowTxState.getCurrentRowLock().getCurrentTimestmap();
		for (int i=0;i<remaining.size();i++){
			byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
			
			TMutation mutation = remaining.get(i);
			switch (mutation.getType()) {
			case PUT:{
				TRowLock newRowLock = rowTxState.getCurrentRowLock();
				newRowLock.setCurrentTimestmap(currentTimestamp + i + 1);
				newRowLock.setMutations(remaining.subList(i + 1, remaining.size()));
				newRowLock.setTimeout(System.currentTimeMillis() + ROW_LOCK_TIMEOUT);
				Put put = new Put(row);
				put.add(LOCK_FAMILY, LOCK_QUALIFIER, newRowLock.getCurrentTimestmap(), HaeinsaThriftUtils.serialize(newRowLock));
				for (TKeyValue kv : mutation.getPut().getValues()){
					put.add(kv.getKey().getFamily(), kv.getKey().getQualifier(), newRowLock.getCurrentTimestmap(), kv.getValue());
				}
				if (!table.checkAndPut(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, put)){
					// 실패하는 경우는 다른 쪽에서 row의 lock을 획득했으므로 충돌이 났다고 처리한다.
					throw new ConflictException("can't acquire row's lock");	
				}else{
					rowTxState.setCurrentRowLock(newRowLock);
				}
				break;
			}
			
			case REMOVE:{
				Delete delete = new Delete(row);
				for (ByteBuffer removeFamily : mutation.getRemove().getRemoveFamilies()){
					delete.deleteFamily(removeFamily.array(), currentTimestamp + i + 1);
				}
				for (TCellKey removeCell : mutation.getRemove().getRemoveCells()){
					delete.deleteColumns(removeCell.getFamily(), removeCell.getQualifier(), currentTimestamp + i + 1);
				}
				if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, delete)){
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
			transaction.abort();
			// 실패하는 경우는 다른 쪽에서 row의 lock을 획득했으므로 충돌이 났다고 처리한다.
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
			return HaeinsaThriftUtils.deserialize(null);
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
		newRowLock.setMutationsIsSet(false);
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
	public void deletePrewritten(RowTransactionState rowTxState, byte[] row)
			throws IOException {
		if (rowTxState.getCurrentRowLock().getPrewrittenSize() == 0){
			return;
		}
		byte[] currentRowLockBytes = HaeinsaThriftUtils.serialize(rowTxState.getCurrentRowLock());
		long prewriteTimestamp = rowTxState.getCurrentRowLock().getCurrentTimestmap();
		Delete delete = new Delete(row);
		for (TCellKey cellKey : rowTxState.getCurrentRowLock().getPrewritten()){
			delete.deleteColumn(cellKey.getFamily(), cellKey.getQualifier(), prewriteTimestamp);
		}
		if (!table.checkAndDelete(row, LOCK_FAMILY, LOCK_QUALIFIER, currentRowLockBytes, delete)){
			// 실패하는 경우는 다른 쪽에서 primary row의 lock을 획득했으므로 충돌이 났다고 처리한다.
			throw new ConflictException("can't acquire primary row's lock");
		}		
	}
	
	@Override
	public HTableInterface getHTable() {
		return table;
	}
}
