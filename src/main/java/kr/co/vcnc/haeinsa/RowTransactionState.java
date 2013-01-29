package kr.co.vcnc.haeinsa;

import java.util.List;

import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import com.google.common.collect.Lists;

public class RowTransactionState {
	private TRowLock originalRowLock;
	private TRowLock currentRowLock;
	private final List<HaeinsaMutation> mutations = Lists.newArrayList();
	private TableTransactionState tableTxState;
	
	RowTransactionState(TableTransactionState tableTxState) {
		this.tableTxState = tableTxState;
	}

	public TRowLock getOriginalRowLock() {
		return originalRowLock;
	}

	public void setOriginalRowLock(TRowLock originalRowLock) {
		this.originalRowLock = originalRowLock;
	}

	public TRowLock getCurrentRowLock() {
		return currentRowLock;
	}

	public void setCurrentRowLock(TRowLock currentRowLock) {
		this.currentRowLock = currentRowLock;
	}

	public List<HaeinsaMutation> getMutations() {
		return mutations;
	}
	
	public void addMutation(HaeinsaMutation mutation) {
		if (mutations.size() <= 0){
			mutations.add(mutation);
		} else {
			HaeinsaMutation lastMutation = mutations.get(mutations.size() - 1);
			if (lastMutation.getClass() != mutation.getClass()){
				mutations.add(mutation);
			}else{
				lastMutation.getFamilyMap().putAll(mutation.getFamilyMap());
			}
		}
	}
	
	public TableTransactionState getTableTxState() {
		return tableTxState;
	}
}
