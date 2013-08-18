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

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingObject;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Fowarding Object for HaeinsaTable
 */
public class ForwardingHaeinsaTable extends ForwardingObject implements HaeinsaTableIfaceInternal {
	private final HaeinsaTableIfaceInternal delegate;

	/**
	 * Constructor
	 * @param haeinsaTable HaeinsaTable
	 */
	public ForwardingHaeinsaTable(HaeinsaTableIface haeinsaTable) {
		Preconditions.checkArgument(haeinsaTable instanceof HaeinsaTableIfaceInternal);
		this.delegate = (HaeinsaTableIfaceInternal) haeinsaTable;
	}

	@Override
	protected HaeinsaTableIfaceInternal delegate() {
		return delegate;
	}

	@Override
	public void commitSingleRowPutOnly(HaeinsaRowTransaction rowState, byte[] row) throws IOException {
		delegate().commitSingleRowPutOnly(rowState, row);
	}

	@Override
	public void checkSingleRowLock(HaeinsaRowTransaction rowState, byte[] row) throws IOException {
		delegate().checkSingleRowLock(rowState, row);
	}

	@Override
	public void prewrite(HaeinsaRowTransaction rowState, byte[] row, boolean isPrimary) throws IOException {
		delegate().prewrite(rowState, row, isPrimary);
	}

	@Override
	public void applyMutations(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		delegate().applyMutations(rowTxState, row);
	}

	@Override
	public void makeStable(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		delegate().makeStable(rowTxState, row);
	}

	@Override
	public void commitPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		delegate().commitPrimary(rowTxState, row);
	}

	@Override
	public TRowLock getRowLock(byte[] row) throws IOException {
		return delegate().getRowLock(row);
	}

	@Override
	public void abortPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		delegate().abortPrimary(rowTxState, row);
	}

	@Override
	public void deletePrewritten(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException {
		delegate().deletePrewritten(rowTxState, row);
	}

	@Override
	public byte[] getTableName() {
		return delegate().getTableName();
	}

	@Override
	public Configuration getConfiguration() {
		return delegate().getConfiguration();
	}

	@Override
	public HTableDescriptor getTableDescriptor() throws IOException {
		return delegate().getTableDescriptor();
	}

	@Override
	public HaeinsaResult get(@Nullable HaeinsaTransaction tx, HaeinsaGet get) throws IOException {
		return delegate().get(tx, get);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family) throws IOException {
		return delegate().getScanner(tx, family);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family, byte[] qualifier) throws IOException {
		return delegate().getScanner(tx, family, qualifier);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, HaeinsaScan scan) throws IOException {
		return delegate().getScanner(tx, scan);
	}

	@Override
	public HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, HaeinsaIntraScan intraScan) throws IOException {
		return delegate().getScanner(tx, intraScan);
	}

	@Override
	public void put(HaeinsaTransaction tx, HaeinsaPut put) throws IOException {
		delegate().put(tx, put);
	}

	@Override
	public void put(HaeinsaTransaction tx, List<HaeinsaPut> puts) throws IOException {
		delegate().put(tx, puts);
	}

	@Override
	public void delete(HaeinsaTransaction tx, HaeinsaDelete delete) throws IOException {
		delegate().delete(tx, delete);
	}

	@Override
	public void delete(HaeinsaTransaction tx, List<HaeinsaDelete> deletes) throws IOException {
		delegate().delete(tx, deletes);
	}

	@Override
	public void close() throws IOException {
		delegate().close();
	}
}
