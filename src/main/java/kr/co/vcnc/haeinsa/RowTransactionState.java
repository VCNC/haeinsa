package kr.co.vcnc.haeinsa;

import java.util.List;

import kr.co.vcnc.haeinsa.thrift.RowLock;

import org.apache.hadoop.hbase.client.Mutation;

import com.google.common.collect.Lists;

public class RowTransactionState {
	private RowLock originalRowLock;
	private RowLock currentRowLock;
	private List<Mutation> mutations = Lists.newArrayList();
	private TableTransactionState tableTxState;
	
	RowTransactionState(TableTransactionState tableTxState) {
		this.tableTxState = tableTxState;
	}

	public RowLock getOriginalRowLock() {
		return originalRowLock;
	}

	public void setOriginalRowLock(RowLock originalRowLock) {
		this.originalRowLock = originalRowLock;
	}

	public RowLock getCurrentRowLock() {
		return currentRowLock;
	}

	public void setCurrentRowLock(RowLock currentRowLock) {
		this.currentRowLock = currentRowLock;
	}

	public List<Mutation> getMutations() {
		return mutations;
	}
	
	public TableTransactionState getTableTxState() {
		return tableTxState;
	}
}
