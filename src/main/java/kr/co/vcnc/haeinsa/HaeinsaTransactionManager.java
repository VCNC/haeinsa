package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

/**
 * {@link HaeinsaTransaction} 을 관리하는 상위 객체이다.
 * {@link HaeinsaTablePool} 을 가지고 있어서 
 * 사용자나 {@link HaeinsaTransaction} 이 {@link HaeinsaTable} 을 통해서 HBase 에 접근하여 
 * transaction 을 수행할 때 tablePool 을 제공하는 역할을 한다.
 * 
 * <p> 또한 실패한 transaction 을 HBase 에 기록되어 있는 TRowLock 으로부터 복원할 수 있는 method 도 제공한다.
 * 
 * @author Youngmok Kim
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
	 * PrimaryRowKey 와 PrimaryRowLock 정보도 HBase 에서 읽어서 복원한다.
	 * <p>This method is thread-safe.
	 * @param tableName TableName of Transaction to recover. 
	 * @param row Row of Transaction to recover.
	 * @return Transaction instance if there is any ongoing Transaction on row, return null otherwise. 
	 * @throws IOException
	 */
	protected @Nullable HaeinsaTransaction getTransaction(byte[] tableName, byte[] row) throws IOException {
		TRowLock startUnstableRowLock = getUnstableRowLock(tableName, row);
		
		if (startUnstableRowLock == null){
			//	There is no on-going transaction on row.
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
	 * @param tableName
	 * @param row
	 * @return null if TRowLock is {@link TRowLockState#STABLE}, otherwise return rowLock from HBase.
	 * @throws IOException
	 */
	private TRowLock getUnstableRowLock(byte[] tableName, byte[] row) throws IOException {
		HaeinsaTable table = (HaeinsaTable) tablePool.getTable(tableName);
		TRowLock rowLock = null;
		try{
			//	access to HBase
			 rowLock = table.getRowLock(row);
		} finally {
			table.close();
		}
		if (rowLock.getState() == TRowLockState.STABLE){
			return null;
		}else{
			return rowLock;
		}
	}
	
	/**
	 * 실패한 Transaction 을 HBase 로부터 primary row 의 TRowLock 정보를 읽어와서 복원하는 데 사용한다.
	 * Secondary row 의 transaction 정보는 {@link #addSecondaryRowLock(HaeinsaTransaction, TRowKey)} 에서 읽어온다.
	 * 단, 이 method 를 통해서 만들어진 HaeinsaTransaction 의 RowTransaction 들은 mutations 값이 제대로 설정되어 있지 않다.
	 *  
	 * @param rowKey
	 * @param primaryRowLock
	 * @return
	 * @throws IOException
	 */
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
	
	/**
	 * primary row 의 lock 으로부터 유추된 secondary row 들의 transaction 정보를 복원하기 위해서 사용된다. 
	 * 만약 해당 secondary row 가 stable 한 상태라면 transaction 에 추가되지 않으며,
	 * 또한 secondary row 가 stable 이 아니더라도 commitTimestamp 가 다르면 다른 transaction 에 의해서 lock 이 된 상태이기 때문에
	 * 역시 transaction 에 추가되지 않는다.
	 * <p> {@link #getTransactionFromPrimary(TRowKey, TRowLock)} 와 마찬가지로, 이 method 를 통해서 
	 * 추가된 secondary row 들의 rowTransaction 에는 mutations 변수가 제대로 설정되어 있지 않다. 
	 * 
	 * @param transaction
	 * @param rowKey
	 * @throws IOException
	 */
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
