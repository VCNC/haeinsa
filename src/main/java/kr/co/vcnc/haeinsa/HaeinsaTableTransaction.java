/**
 * Copyright (C) 2013 VCNC, inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import kr.co.vcnc.haeinsa.thrift.TRowLocks;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

/**
 * Contains Transaction information of single Table.
 * <p>
 * It have map of {byte[] row -> {@link HaeinsaRowTransaction} and reference to
 * {@link HaeinsaTransaction}
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
	 * Return rowTransaction which this instance contains in rowStates map.
	 * If there is no rowTransaction for this row, then create new one and return it.
	 * Returned rowTransaction is always saved in rowStates.
	 * <p>
	 * There are three possible states of TRowLock of {@link HaeinsaRowTransaction} which returned by this method.
	 * <p>
	 * 1. When get {@link HaeinsaRowTransaction} which is already contained in rowStates
	 * - Should not change {@link HaeinsaRowTransaction#current} manually.
	 * <p>
	 * 2. When rowTransaction is newly created by this method and {@link TRowLock} associated with the row exists
	 * - Use {@link HaeinsaRowTransaction#setCurrent()} to set current field of rowTransaction.
	 * <p>
	 * 3. When rowTransaction is newly created by this method and there is no associated {@link TRowLock}
	 * - Use {@link TRowLocks#serialize(null)} method to set default {@link TRowLock} to current field of rowTransaction.
	 *
	 * @param row
	 * @return RowTransaction - {@link HaeinsaRowTransaction} which contained in
	 *         this instance.
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
