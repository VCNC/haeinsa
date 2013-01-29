package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.nio.ByteBuffer;

import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

public class TransactionManager {
	private final TablePool tablePool;
	
	public TransactionManager(TablePool tablePool){
		this.tablePool = tablePool;
	}
	
	public Transaction begin(){
		return new Transaction(this);
	}
	
	public Transaction getTransaction(byte[] tableName, byte[] row) throws IOException {
		TRowLock startRowLock = getUnstableRowLock(tableName, row);
		
		if (startRowLock == null){
			return null;
		}
		
		TRowLock primaryRowLock = null;
		TRowKey primaryRowKey = null;
		if (startRowLock.getSecondariesSize() > 0){
			// 이 Row가 Primary Row
			primaryRowKey = new TRowKey(ByteBuffer.wrap(tableName), ByteBuffer.wrap(row));
			primaryRowLock = startRowLock;
		}else {
			primaryRowKey = startRowLock.getPrimary();
			primaryRowLock = getUnstableRowLock(primaryRowKey.getTableName(), primaryRowKey.getRow());
		}
		if (primaryRowLock == null){
			return null;
		}
		return getTransactionFromPrimary(primaryRowKey, primaryRowLock);
	}
	
	private TRowLock getUnstableRowLock(byte[] tableName, byte[] row) throws IOException {
		HaeinsaTable.PrivateIface table = (HaeinsaTable.PrivateIface) tablePool.getTable(tableName);
		TRowLock rowLock = table.getRowLock(row);
		if (rowLock.getState() == TRowLockState.STABLE){
			return null;
		}else{
			return rowLock;
		}
	}
	
	private Transaction getTransactionFromPrimary(TRowKey rowKey, TRowLock primaryRowLock) throws IOException {
		Transaction transaction = new Transaction(this);
		transaction.setPrimary(rowKey);
		transaction.setCommitTimestamp(primaryRowLock.getCommitTimestamp());
		TableTransactionState primaryTableTxState = transaction.createOrGetTableState(rowKey.getTableName());
		RowTransactionState primaryRowTxState = primaryTableTxState.createOrGetRowState(rowKey.getRow());
		primaryRowTxState.setOriginalRowLock(primaryRowLock);
		primaryRowTxState.setCurrentRowLock(primaryRowLock);
		if (primaryRowLock.getSecondariesSize() > 0){
			for (TRowKey secondaryRow : primaryRowLock.getSecondaries()){
				addSecondaryRowLock(transaction, secondaryRow);
			}
		}
		
		return transaction;
	}
	
	private void addSecondaryRowLock(Transaction transaction, TRowKey rowKey) throws IOException {
		TRowLock rowLock = getUnstableRowLock(rowKey.getTableName(),	rowKey.getRow());
		if (rowLock == null){
			return;
		}
		if (rowLock.getCommitTimestamp() != transaction.getCommitTimestamp()){
			return;
		}
		TableTransactionState tableState = transaction.createOrGetTableState(rowKey.getTableName());
		RowTransactionState rowState = tableState.createOrGetRowState(rowKey.getRow());
		rowState.setCurrentRowLock(rowLock);
		rowState.setOriginalRowLock(rowLock);
	}
		
	public TablePool getTablePool() {
		return tablePool;
	}
}
