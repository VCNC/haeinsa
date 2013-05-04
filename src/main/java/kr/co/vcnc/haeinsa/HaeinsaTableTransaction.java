package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import kr.co.vcnc.haeinsa.thrift.TRowLocks;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Contains Transaction information of single Table.
 * <p>It have map of {byte[] row -> {@link HaeinsaRowTransaction}} and reference to {@link HaeinsaTransaction}
 * @author Youngmok Kim
 *
 */
class HaeinsaTableTransaction {
	private final NavigableMap<byte[], HaeinsaRowTransaction> rowStates = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final HaeinsaTransaction transaction;

	HaeinsaTableTransaction(HaeinsaTransaction transaction) {
		this.transaction = transaction;
	}

	public NavigableMap<byte[], HaeinsaRowTransaction> getRowStates() {
		return rowStates;
	}

	public HaeinsaTransaction getTransaction() {
		return transaction;
	}

	/**
	 * 현재의 TableTransaction 이 들고 있는 RowTransaction 을 return 한다.
	 * 만약 주어진 row 에 해당하는 RowTransaction 이 존재하지 않는다면, TableTransaction 내에 해당 row 의 RowTransaction 을 만들고
	 * 그 RowTransaction 을 내부 map 에 저장한 후에 return 한다.
	 * <p>이 method 를 사용해서 HaeinsaRowTransaction 을 사용하는 유저는 항상 해당 {@link HaeinsaRowTransaction} 이
	 * 적당한 TRowLock 정보를 들고 있는지 확인해야 한다. 다음 3가지 경우가 있을 수 있다.
	 * <p>1. 이미 존재하는 {@link HaeinsaRowTransaction} 을 가져온 경우 -
	 * current 값을 변경하면 안된다.
	 *
	 * <p>2. 새로 만들어졌고 HBase 에서 가져올 {@link TRowLock} 정보가 있는 경우 -
	 * {@link HaeinsaRowTransaction#setCurrent()} 를 사용하면 된다.
	 *
	 * <p>3. 새로 만들어졌고 HBase 에서 가져올 {@link TRowLock} 정보가 없는 경우 -
	 * {@link TRowLocks#serialize(null)} 을 넣으면 된다.
	 * @param row
	 * @return RowTransaction - {@link HaeinsaRowTransaction} which contained in this instance.
	 */
	public HaeinsaRowTransaction createOrGetRowState(byte[] row) {
		HaeinsaRowTransaction rowState = rowStates.get(row);
		if (rowState == null) {
			rowState = new HaeinsaRowTransaction(this);
			rowStates.put(row, rowState);
		}
		return rowState;
	}
}
