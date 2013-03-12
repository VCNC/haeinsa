package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.nio.ByteBuffer;

import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

/**
 * TODO
 * @author Myungbo Kim
 *
 */
public class HaeinsaTransactionManager {
	private final HaeinsaTablePool tablePool;
	
	/**
	 * Constructor for TransactionManager
	 * @param tablePool HaeinsaTablePool to access HBase.
	 */
	public HaeinsaTransactionManager(HaeinsaTablePool tablePool){
		this.tablePool = tablePool;
	}
	
	/**
	 * Get {@link HaeinsaTransaction} instance which can be used to start new transaction.
	 * <p>This method is thread-safe. 
	 * @return new Transaction instance have reference to this manager instance.
	 */
	public HaeinsaTransaction begin(){
		return new HaeinsaTransaction(this);
	}
	
	/**
	 * Make new {@link HaeinsaTransaction} instance which can be used to recover other failed/uncompleted transaction.
	 * <p>This method is thread-safe.
	 * @param tableName TableName of Transaction to recover. 
	 * @param row Row of Transaction to recover.
	 * @return Transaction instance if there is any ongoing Transaction on row, return null otherwise. 
	 * @throws IOException
	 */
	public HaeinsaTransaction getTransaction(byte[] tableName, byte[] row) throws IOException {
		TRowLock startUnstableRowLock = getUnstableRowLock(tableName, row);
		
		if (startUnstableRowLock == null){
			return null;
		}
		
		TRowLock primaryRowLock = null;
		TRowKey primaryRowKey = null;
		if (!startUnstableRowLock.isSetPrimary()){
			// 이 Row가 Primary Row
			primaryRowKey = new TRowKey(ByteBuffer.wrap(tableName), ByteBuffer.wrap(row));
			primaryRowLock = startUnstableRowLock;
		}else {
			primaryRowKey = startUnstableRowLock.getPrimary();
			primaryRowLock = getUnstableRowLock(primaryRowKey.getTableName(), primaryRowKey.getRow());
		}
		if (primaryRowLock == null){
			return null;
		}
		return getTransactionFromPrimary(primaryRowKey, primaryRowLock);
	}
	
	/**
	 * 
	 * @param tableName
	 * @param row
	 * @return null if TRowLock is {@link TRowLockState#STABLE}, otherwise return rowLock from HBase.
	 * @throws IOException
	 */
	private TRowLock getUnstableRowLock(byte[] tableName, byte[] row) throws IOException {
		HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableName);
		TRowLock rowLock = table.getRowLock(row);
		if (rowLock.getState() == TRowLockState.STABLE){
			return null;
		}else{
			return rowLock;
		}
	}
	
	private HaeinsaTransaction getTransactionFromPrimary(TRowKey rowKey, TRowLock primaryRowLock) throws IOException {
		HaeinsaTransaction transaction = new HaeinsaTransaction(this);
		transaction.setPrimary(rowKey);
		transaction.setCommitTimestamp(primaryRowLock.getCommitTimestamp());
		HaeinsaTableTransaction primaryTableTxState = transaction.createOrGetTableState(rowKey.getTableName());
		HaeinsaRowTransaction primaryRowTxState = primaryTableTxState.createOrGetRowState(rowKey.getRow());
		primaryRowTxState.setCurrent(primaryRowLock);
		if (primaryRowLock.getSecondariesSize() > 0){
			for (TRowKey secondaryRow : primaryRowLock.getSecondaries()){
				addSecondaryRowLock(transaction, secondaryRow);
			}
		}
		
		return transaction;
	}
	
	private void addSecondaryRowLock(HaeinsaTransaction transaction, TRowKey rowKey) throws IOException {
		TRowLock unstableRowLock = getUnstableRowLock(rowKey.getTableName(),	rowKey.getRow());
		if (unstableRowLock == null){
			return;
		}
		// commitTimestamp가 다르면, 다른 Transaction 이므로 추가하면 안됨  
		if (unstableRowLock.getCommitTimestamp() != transaction.getCommitTimestamp()){
			return;
		}
		HaeinsaTableTransaction tableState = transaction.createOrGetTableState(rowKey.getTableName());
		HaeinsaRowTransaction rowState = tableState.createOrGetRowState(rowKey.getRow());
		rowState.setCurrent(unstableRowLock);
	}

	/**
	 * @return HaeinsaTablePool contained in TransactionManager
	 */
	public HaeinsaTablePool getTablePool() {
		return tablePool;
	}
}
