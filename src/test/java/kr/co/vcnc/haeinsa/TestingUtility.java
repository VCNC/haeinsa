/**
 * Copyright (C) 2013-2014 VCNC Inc.
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
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;

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

    /**
     * Check if there is lock in specific row in table
     *
     * @param table given HTableInterface
     * @param row given row key
     * @return true if TRowLock exist in specific row
     * @throws IOException exception occurred when retrieving lock from HBase
     */
    public static boolean checkLockExist(HTableInterface table, byte[] row) throws IOException {
        return getLock(table, row) != null;
    }

    /**
     * Get Lock from given row in table
     *
     * @param table given HTableInterface
     * @param row given row key
     * @return byte array which represents lock
     * @throws IOException exception occurred when retrieving lock from HBase
     */
    public static byte[] getLock(HTableInterface table, byte[] row) throws IOException {
        return table.get(new Get(row).addColumn(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER))
                .getValue(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER);
    }

    /**
     * Check whether lock of the specific row is changed from old lock.
     *
     * @param table specific HTableInterface
     * @param row row key of the specific row
     * @param oldLock byte array of the old lock
     * @return if lock of the given row is changed
     * @throws IOException exception occurred when retrieving lock from HBase
     */
    public static boolean checkLockChanged(HTableInterface table, byte[] row, byte[] oldLock) throws IOException {
        return !Bytes.equals(getLock(table, row), oldLock);
    }
}
