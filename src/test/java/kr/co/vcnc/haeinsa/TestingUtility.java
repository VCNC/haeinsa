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
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;

import com.beust.jcommander.internal.Lists;

public final class TestingUtility {

	private TestingUtility() {}

	/**
	 * Create {@link HaeinsaTablePool} instance for testing
	 *
	 * @param threadPool instance of {@link ExecutorService} to use
	 * @return instance of {@link HaeinsaTablePool}
	 */
	public static HaeinsaTablePool createHaeinsaTablePool(Configuration conf, final ExecutorService threadPool) {
		return new HaeinsaTablePool(conf, 128, new DefaultHaeinsaTableIfaceFactory(new HTableInterfaceFactory() {
			@Override
			public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
				try {
					return new HTable(tableName, HConnectionManager.getConnection(config), threadPool);
				} catch (ZooKeeperConnectionException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}
			@Override
			public void releaseHTableInterface(HTableInterface table) throws IOException {
				table.close();
			}
		}));
	}

	public static void cleanTable(Configuration conf, String tableName) throws Exception {
		HTablePool hbasePool = new HTablePool(conf, 128, PoolType.Reusable);
		HTableInterface hTestTable = hbasePool.getTable("test");

		Scan scan = new Scan();
		List<byte[]> rows = Lists.newArrayList();
		try (ResultScanner scanner = hTestTable.getScanner(scan)) {
			for (Result result : scanner) {
				rows.add(result.getRow());
			}
		}

		for (byte[] row : rows) {
			Delete delete = new Delete(row);
			delete.deleteFamily(Bytes.toBytes("data"));
			hTestTable.delete(delete);
		}

		hbasePool.close();
		hTestTable.close();
	}
}
