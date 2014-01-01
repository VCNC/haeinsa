/**
 * Copyright (C) 2014 VCNC Inc.
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

import static kr.co.vcnc.haeinsa.TestingUtility.checkLockExist;

import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for scheme migration from HBase-only cluster.
 * This test ensures migration logic works correctly
 * by simulating migration from HBase-only schema to Haeinsa schema.
 */
public class HBaseMigrationTest extends HaeinsaTestBase {

    /**
     * Lock should be NOT created after HaeinsaGet executed.
     * HaeinsaGet operation should not modify data of the table.
     */
    @Test
    public void testMigrationByGet() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put = new Put(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Perform HaeinsaGet to the same cell in the table.
        // Lock should NOT be created since this is just a HaeinsaGet operation.
        // But data should be properly retrieved.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result = testTable.get(tx, get);
            tx.rollback();
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1"));
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Lock should be created after HaeinsaPut executed.
     */
    @Test
    public void testMigrationByPut() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put = new Put(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Perform HaeinsaPut data to the same cell in the table.
        // Lock should be created automatically by Haeinsa.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value2"));
            testTable.put(tx, put);
            tx.commit();
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Check if data is properly written to HBase.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result = testTable.get(tx, get);
            tx.rollback();
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value2"));
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Lock should be created after HaeinsaDelete executed.
     */
    @Test
    public void testMigrationByDelete() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put = new Put(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Perform HaeinsaDelete data to the same cell in the table.
        // Lock should be created automatically by Haeinsa.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaDelete delete = new HaeinsaDelete(Bytes.toBytes("row1"));
            delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            testTable.delete(tx, delete);
            tx.commit();
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Check if data is properly written to HBase.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result = testTable.get(tx, get);
            tx.rollback();
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertNull(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")));
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Test migration logic for that transaction that executes multiple-row Scan.
     * Lock should be not created for rows of Scan operation.
     */
    @Test
    public void testMigrationByInterRowScan() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put1 = new Put(Bytes.toBytes("row1"));
            put1.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
            hTestTable.put(put1);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));

            Put put2 = new Put(Bytes.toBytes("row2"));
            put2.add(Bytes.toBytes("data"), Bytes.toBytes("col2"), Bytes.toBytes("value2"));
            hTestTable.put(put2);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row2")));

            Put put3 = new Put(Bytes.toBytes("row3"));
            put3.add(Bytes.toBytes("data"), Bytes.toBytes("col3"), Bytes.toBytes("value3"));
            hTestTable.put(put3);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row3")));
        }

        // Perform HaeinsaScan by multiple row in the table.
        // Lock should be NOT created since Scan is just multiple get operation.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaScan scan = new HaeinsaScan();
            scan.setStartRow(Bytes.toBytes("row1"));
            scan.setStopRow(Bytes.toBytes("row3"));
            HaeinsaResultScanner resultScanner = testTable.getScanner(tx, scan);
            Iterator<HaeinsaResult> iter = resultScanner.iterator();
            while (iter.hasNext()) {
                iter.next();
            }
            resultScanner.close();
            tx.commit();

            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row2")));
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row3")));
        }

        // Check if data is properly exists in HBase.
        {
            HaeinsaTransaction tx = tm.begin();

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result1 = testTable.get(tx, get1);
            Assert.assertEquals(result1.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1"));

            HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("row2"));
            get2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
            HaeinsaResult result2 = testTable.get(tx, get2);
            Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("col2")), Bytes.toBytes("value2"));

            HaeinsaGet get3 = new HaeinsaGet(Bytes.toBytes("row3"));
            get3.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col3"));
            HaeinsaResult result3 = testTable.get(tx, get3);
            Assert.assertEquals(result3.getValue(Bytes.toBytes("data"), Bytes.toBytes("col3")), Bytes.toBytes("value3"));

            tx.rollback();
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Test migration logic for that transaction that executes multiple-column Scan.
     * Lock should be not created for row of Scan operation.
     */
    @Test
    public void testMigrationByIntraRowScan() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put = new Put(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1-1"), Bytes.toBytes("value1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1-2"), Bytes.toBytes("value2"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1-3"), Bytes.toBytes("value3"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Perform HaeinsaScan by multiple column of single row in the table.
        // Lock should be NOT created since Scan is just multiple get operation.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaIntraScan intraScan = new HaeinsaIntraScan(
                    Bytes.toBytes("row1"),
                    Bytes.toBytes("col1-1"), true,
                    Bytes.toBytes("col1-3"), true);
            intraScan.setBatch(1);

            HaeinsaResultScanner resultScanner = testTable.getScanner(tx, intraScan);
            Iterator<HaeinsaResult> iter = resultScanner.iterator();
            Assert.assertEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-1")), Bytes.toBytes("value1"));
            Assert.assertEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-2")), Bytes.toBytes("value2"));
            Assert.assertEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-3")), Bytes.toBytes("value3"));
            resultScanner.close();
            tx.commit();

            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Check if data is properly exists in HBase.
        {
            HaeinsaTransaction tx = tm.begin();

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1-1"));
            HaeinsaResult result1 = testTable.get(tx, get1);
            Assert.assertEquals(result1.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-1")), Bytes.toBytes("value1"));

            HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("row1"));
            get2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1-2"));
            HaeinsaResult result2 = testTable.get(tx, get2);
            Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-2")), Bytes.toBytes("value2"));

            HaeinsaGet get3 = new HaeinsaGet(Bytes.toBytes("row1"));
            get3.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1-3"));
            HaeinsaResult result3 = testTable.get(tx, get3);
            Assert.assertEquals(result3.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-3")), Bytes.toBytes("value3"));

            tx.rollback();
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Test migration logic for that transaction that executes Get and Put atomically.
     * Lock should be not created for row of Get operation, but created for row of Put operation.
     */
    @Test
    public void testMigrationByMixedPutAndGet() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        // Row1 is for HaeinsaGet Row2 is for HaeinsaPut.
        {
            Put put = new Put(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row2")));
        }

        // HaeinsaGet from Row1 and HaeinsaPut to Row2 by single transaction.
        // There should be no lock in Row1 but exists in Row2.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result = testTable.get(tx, get);
            Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1"));

            HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row2"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col2"), Bytes.toBytes("value2"));
            testTable.put(tx, put);
            tx.commit();

            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row2")));
        }

        // Check if data is properly exists in HBase
        {
            HaeinsaTransaction tx = tm.begin();

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result = testTable.get(tx, get1);

            Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1"));
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));

            HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("row2"));
            get2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
            result = testTable.get(tx, get2);

            Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col2")), Bytes.toBytes("value2"));
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row2")));

            tx.rollback();
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Test migration logic for that transaction that executes Put and Delete atomically.
     * Lock should be created for both rows of Put and Delete operation
     */
    @Test
    public void testMigrationByMixedPutAndDelete() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put1 = new Put(Bytes.toBytes("row1"));
            put1.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
            hTestTable.put(put1);

            Put put2 = new Put(Bytes.toBytes("row2"));
            put2.add(Bytes.toBytes("data"), Bytes.toBytes("col2"), Bytes.toBytes("value2"));
            hTestTable.put(put2);

            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row2")));
        }

        // HaeinsaGet from Row1 and HaeinsaPut to Row2 by single transaction.
        // There should be no lock in Row1 but exists in Row2.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaDelete delete = new HaeinsaDelete(Bytes.toBytes("row1"));
            delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            testTable.delete(tx, delete);

            HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row2"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col2"), Bytes.toBytes("value3"));
            testTable.put(tx, put);
            tx.commit();

            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row2")));
        }

        // Check if data is properly exists in HBase
        {
            HaeinsaTransaction tx = tm.begin();

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result = testTable.get(tx, get1);

            Assert.assertNull(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")));
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row1")));

            HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("row2"));
            get2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
            result = testTable.get(tx, get2);

            Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col2")), Bytes.toBytes("value3"));
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row2")));

            tx.rollback();
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Test migration logic for that transaction that executes multiple-row Scan and Put atomically.
     * Lock should be not created for rows of Scan operation, but created for row of Put operation.
     */
    @Test
    public void testMigrationByMixedInterRowScanAndPut() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put = null;

            put = new Put(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));

            put = new Put(Bytes.toBytes("row2"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col2"), Bytes.toBytes("value2"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row2")));

            put = new Put(Bytes.toBytes("row3"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col3"), Bytes.toBytes("value3"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row3")));
        }

        // Perform HaeinsaScan by multiple row in the table.
        // Lock should be NOT created since Scan is just multiple get operation.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaScan scan = new HaeinsaScan();
            scan.setStartRow(Bytes.toBytes("row1"));
            scan.setStopRow(Bytes.toBytes("row3"));
            Iterator<HaeinsaResult> iter = testTable.getScanner(tx, scan).iterator();
            while (iter.hasNext()) {
                iter.next();
            }

            HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row4"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col4"), Bytes.toBytes("value4"));
            testTable.put(tx, put);
            tx.commit();

            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row2")));
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row3")));
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row4")));
        }

        // Check if data is properly exists in HBase.
        {
            HaeinsaTransaction tx = tm.begin();

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
            HaeinsaResult result1 = testTable.get(tx, get1);
            Assert.assertEquals(result1.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1"));

            HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("row2"));
            get2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
            HaeinsaResult result2 = testTable.get(tx, get2);
            Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("col2")), Bytes.toBytes("value2"));

            HaeinsaGet get3 = new HaeinsaGet(Bytes.toBytes("row3"));
            get3.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col3"));
            HaeinsaResult result3 = testTable.get(tx, get3);
            Assert.assertEquals(result3.getValue(Bytes.toBytes("data"), Bytes.toBytes("col3")), Bytes.toBytes("value3"));

            HaeinsaGet get4 = new HaeinsaGet(Bytes.toBytes("row4"));
            get4.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col4"));
            HaeinsaResult result4 = testTable.get(tx, get4);
            Assert.assertEquals(result4.getValue(Bytes.toBytes("data"), Bytes.toBytes("col4")), Bytes.toBytes("value4"));

            tx.rollback();
        }

        hTestTable.close();
        testTable.close();
    }

    /**
     * Test migration logic for that transaction that executes multiple-column Scan and Put atomically.
     * Lock should be not created for row of Scan operation, but created for row of Put operation.
     */
    @Test
    public void testMigrationByMixedIntraRowScanAndPut() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable(currentTableName());
        final HTableInterface hTestTable = CLUSTER.getHbaseTable(currentTableName());

        // Put sample data to HBase cluster by primitive HBase operation.
        // Lock will not created because it is just a primitive HBase operation.
        {
            Put put = new Put(Bytes.toBytes("row1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1-1"), Bytes.toBytes("value1"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1-2"), Bytes.toBytes("value2"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col1-3"), Bytes.toBytes("value3"));
            hTestTable.put(put);
            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
        }

        // Perform HaeinsaScan by multiple column of single row in the table.
        // Lock should be NOT created since Scan is just multiple get operation.
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaIntraScan intraScan = new HaeinsaIntraScan(
                    Bytes.toBytes("row1"),
                    Bytes.toBytes("col1"), true,
                    Bytes.toBytes("col1-3"), true);
            intraScan.setBatch(1);

            HaeinsaResultScanner resultScanner = testTable.getScanner(tx, intraScan);
            Iterator<HaeinsaResult> iter = resultScanner.iterator();

            Assert.assertEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-1")), Bytes.toBytes("value1"));
            Assert.assertEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-2")), Bytes.toBytes("value2"));
            Assert.assertEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-3")), Bytes.toBytes("value3"));
            resultScanner.close();

            HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row2"));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("col2"), Bytes.toBytes("value4"));
            testTable.put(tx, put);
            tx.commit();

            Assert.assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
            Assert.assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row2")));
        }

        // Check if data is properly exists in HBase.
        {
            HaeinsaTransaction tx = tm.begin();

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1-1"));
            HaeinsaResult result1 = testTable.get(tx, get1);
            Assert.assertEquals(result1.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-1")), Bytes.toBytes("value1"));

            HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("row1"));
            get2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1-2"));
            HaeinsaResult result2 = testTable.get(tx, get2);
            Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-2")), Bytes.toBytes("value2"));

            HaeinsaGet get3 = new HaeinsaGet(Bytes.toBytes("row1"));
            get3.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1-3"));
            HaeinsaResult result3 = testTable.get(tx, get3);
            Assert.assertEquals(result3.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1-3")), Bytes.toBytes("value3"));

            HaeinsaGet get4 = new HaeinsaGet(Bytes.toBytes("row2"));
            get4.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
            HaeinsaResult result4 = testTable.get(tx, get4);
            Assert.assertEquals(result4.getValue(Bytes.toBytes("data"), Bytes.toBytes("col2")), Bytes.toBytes("value4"));

            tx.rollback();
        }

        hTestTable.close();
        testTable.close();
    }
}
