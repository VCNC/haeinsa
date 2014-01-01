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

import static kr.co.vcnc.haeinsa.TestingUtility.checkLockChanged;
import static kr.co.vcnc.haeinsa.TestingUtility.getLock;

import java.util.Iterator;

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.exception.DanglingRowLockException;
import kr.co.vcnc.haeinsa.thrift.TRowLocks;
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Basic unit test for Haeinsa which consist of basic transaction test, multiple
 * mutations test, conflict and recover test, conflict and abort test,
 * HaeinsaWithoutTx test, and HBase migration test.
 */
public class HaeinsaUnitTest extends HaeinsaTestBase {

    @Test
    public void testTransaction() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testTransaction.test");
        final HaeinsaTableIface logTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testTransaction.log");

        // Test 2 puts tx
        HaeinsaTransaction tx = tm.begin();
        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        testTable.put(tx, put);
        HaeinsaPut testPut = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        testPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, testPut);
        tx.commit();

        tx = tm.begin();
        HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("ymkim"));
        HaeinsaResult result = testTable.get(tx, get);
        HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("kjwoo"));
        HaeinsaResult result2 = testTable.get(tx, get2);
        tx.rollback();
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
        Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

        tx = tm.begin();
        put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, put);
        testPut = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        testPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        testTable.put(tx, testPut);
        tx.commit();

        tx = tm.begin();
        get = new HaeinsaGet(Bytes.toBytes("ymkim"));
        result = testTable.get(tx, get);
        get2 = new HaeinsaGet(Bytes.toBytes("kjwoo"));
        result2 = testTable.get(tx, get2);
        tx.rollback();

        Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

        tx = tm.begin();
        HaeinsaScan scan = new HaeinsaScan();
        HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
        result = scanner.next();
        result2 = scanner.next();
        HaeinsaResult result3 = scanner.next();

        Assert.assertNull(result3);
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
        Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
        scanner.close();
        tx.rollback();

        tx = tm.begin();
        put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        testTable.put(tx, put);
        testPut = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        testPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, testPut);
        scan = new HaeinsaScan();
        scanner = testTable.getScanner(tx, scan);
        result = scanner.next();
        result2 = scanner.next();
        result3 = scanner.next();

        Assert.assertNull(result3);
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
        Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
        scanner.close();
        tx.rollback();

        tx = tm.begin();
        put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        testTable.put(tx, put);
        testPut = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        testPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, testPut);
        scan = new HaeinsaScan();
        scan.setStartRow(Bytes.toBytes("kjwoo"));
        scan.setStopRow(Bytes.toBytes("ymkim"));
        scanner = testTable.getScanner(tx, scan);
        result = scanner.next();
        result2 = scanner.next();
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
        Assert.assertNull(result2);
        scanner.close();
        tx.rollback();

        tx = tm.begin();
        HaeinsaDelete delete1 = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete1.deleteFamily(Bytes.toBytes("data"));

        HaeinsaDelete delete2 = new HaeinsaDelete(Bytes.toBytes("kjwoo"));
        delete2.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));

        testTable.delete(tx, delete1);
        testTable.delete(tx, delete2);

        testPut = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        testPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, testPut);

        scan = new HaeinsaScan();
        scanner = testTable.getScanner(tx, scan);
        result = scanner.next();
        result2 = scanner.next();

        Assert.assertNull(result2);
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
        scanner.close();

        tx.commit();

        tx = tm.begin();
        get = new HaeinsaGet(Bytes.toBytes("ymkim"));
        result = testTable.get(tx, get);
        get2 = new HaeinsaGet(Bytes.toBytes("kjwoo"));
        result2 = testTable.get(tx, get2);
        tx.rollback();

        Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

        Assert.assertTrue(result.isEmpty());
        Assert.assertFalse(result2.isEmpty());

        tx = tm.begin();
        put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, put);
        testPut = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        testPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        testTable.put(tx, testPut);
        tx.commit();

        tx = tm.begin();
        get = new HaeinsaGet(Bytes.toBytes("ymkim"));
        result = testTable.get(tx, get);
        get2 = new HaeinsaGet(Bytes.toBytes("kjwoo"));
        result2 = testTable.get(tx, get2);
        tx.rollback();

        Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

        tx = tm.begin();
        delete1 = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete1.deleteFamily(Bytes.toBytes("data"));

        delete2 = new HaeinsaDelete(Bytes.toBytes("kjwoo"));
        delete2.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));

        testTable.delete(tx, delete1);
        testTable.delete(tx, delete2);

        tx.commit();

        // test Table-cross transaction & multi-Column transaction
        tx = tm.begin();
        put = new HaeinsaPut(Bytes.toBytes("previousTime"));
        put.add(Bytes.toBytes("raw"), Bytes.toBytes("time-0"), Bytes.toBytes("log-value-1"));
        logTable.put(tx, put);
        put = new HaeinsaPut(Bytes.toBytes("row-0"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("time-0"), Bytes.toBytes("data-value-1"));
        put.add(Bytes.toBytes("meta"), Bytes.toBytes("time-0"), Bytes.toBytes("meta-value-1"));
        testTable.put(tx, put);
        tx.commit();

        // check tx result
        tx = tm.begin();
        get = new HaeinsaGet(Bytes.toBytes("previousTime"));
        get.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("time-0"));
        Assert.assertEquals(logTable.get(tx, get).getValue(Bytes.toBytes("raw"), Bytes.toBytes("time-0")), Bytes.toBytes("log-value-1"));
        get = new HaeinsaGet(Bytes.toBytes("row-0"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("time-0"));
        Assert.assertEquals(testTable.get(tx, get).getValue(Bytes.toBytes("data"), Bytes.toBytes("time-0")), Bytes.toBytes("data-value-1"));
        get = new HaeinsaGet(Bytes.toBytes("row-0"));
        get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("time-0"));
        Assert.assertEquals(testTable.get(tx, get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("time-0")), Bytes.toBytes("meta-value-1"));
        tx.rollback();

        // clear test - table
        tx = tm.begin();
        scan = new HaeinsaScan();
        scanner = testTable.getScanner(tx, scan);
        Iterator<HaeinsaResult> iter = scanner.iterator();
        while (iter.hasNext()) {
            result = iter.next();
            for (HaeinsaKeyValue kv : result.list()) {
                kv.getRow();
                // delete specific kv - delete only if it's not lock family
                HaeinsaDelete delete = new HaeinsaDelete(kv.getRow());
                // should not return lock by scanner
                Assert.assertFalse(Bytes.equals(kv.getFamily(), HaeinsaConstants.LOCK_FAMILY));
                delete.deleteColumns(kv.getFamily(), kv.getQualifier());
                testTable.delete(tx, delete);
            }
        }
        tx.commit();
        scanner.close();

        // clear log - table
        tx = tm.begin();
        scan = new HaeinsaScan();
        scanner = logTable.getScanner(tx, scan);
        iter = scanner.iterator();
        while (iter.hasNext()) {
            result = iter.next();
            for (HaeinsaKeyValue kv : result.list()) {
                kv.getRow();
                // delete specific kv - delete only if it's not lock family
                HaeinsaDelete delete = new HaeinsaDelete(kv.getRow());
                // should not return lock by scanner
                Assert.assertFalse(Bytes.equals(kv.getFamily(), HaeinsaConstants.LOCK_FAMILY));
                delete.deleteColumns(kv.getFamily(), kv.getQualifier());
                logTable.delete(tx, delete);
            }
        }
        tx.commit();
        scanner.close();

        // check whether table is clear - testTable
        tx = tm.begin();
        scan = new HaeinsaScan();
        scanner = testTable.getScanner(tx, scan);
        iter = scanner.iterator();
        Assert.assertFalse(iter.hasNext());
        tx.rollback();
        scanner.close();
        // check whether table is clear - logTable
        tx = tm.begin();
        scan = new HaeinsaScan();
        scanner = logTable.getScanner(tx, scan);
        iter = scanner.iterator();
        Assert.assertFalse(iter.hasNext());
        tx.rollback();
        scanner.close();

        testTable.close();
        logTable.close();
    }

    @Test
    public void testMultiPutAndMultiDelete() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testMultiPutAndMultiDelete.test");

        HaeinsaTransaction tx = tm.begin();

        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678+1"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("email"), Bytes.toBytes("ymkim+1@vcnc.co.kr"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("Youngmok Kim+1"));
        testTable.put(tx, put);

        HaeinsaDelete delete = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));
        testTable.delete(tx, delete);

        put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678+2"));
        testTable.put(tx, put);

        delete = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("name"));
        testTable.delete(tx, delete);

        put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("city"), Bytes.toBytes("Seoul"));
        testTable.put(tx, put);

        tx.commit();

        tx = tm.begin();
        HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("ymkim"));
        HaeinsaResult result = testTable.get(tx, get);
        Assert.assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678+2")));
        Assert.assertNull(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
        Assert.assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("city")), Bytes.toBytes("Seoul")));
        Assert.assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("email")), Bytes.toBytes("ymkim+1@vcnc.co.kr")));
        tx.rollback();

        // clear test - table
        tx = tm.begin();
        HaeinsaScan scan = new HaeinsaScan();
        HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
        Iterator<HaeinsaResult> iter = scanner.iterator();
        while (iter.hasNext()) {
            result = iter.next();
            for (HaeinsaKeyValue kv : result.list()) {
                // delete specific kv - delete only if it's not lock family
                delete = new HaeinsaDelete(kv.getRow());
                // should not return lock by scanner
                Assert.assertFalse(Bytes.equals(kv.getFamily(), HaeinsaConstants.LOCK_FAMILY));
                delete.deleteColumns(kv.getFamily(), kv.getQualifier());
                testTable.delete(tx, delete);
            }
        }
        tx.commit();
        scanner.close();

        testTable.close();
    }

    @Test
    public void testMultiRowReadOnly() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testMultiRowReadOnly.test");

        // Test 2 puts tx
        HaeinsaTransaction tx = tm.begin();
        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        testTable.put(tx, put);
        HaeinsaPut testPut = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        testPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, testPut);
        tx.commit();

        tx = tm.begin();
        HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("ymkim"));
        HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("kjwoo"));
        testTable.get(tx, get1);
        testTable.get(tx, get2);
        tx.commit();

        // clear test - table
        tx = tm.begin();
        HaeinsaScan scan = new HaeinsaScan();
        HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
        Iterator<HaeinsaResult> iter = scanner.iterator();
        while (iter.hasNext()) {
            HaeinsaResult result = iter.next();
            for (HaeinsaKeyValue kv : result.list()) {
                // delete specific kv - delete only if it's not lock family
                HaeinsaDelete delete = new HaeinsaDelete(kv.getRow());
                // should not return lock by scanner
                Assert.assertFalse(Bytes.equals(kv.getFamily(), HaeinsaConstants.LOCK_FAMILY));
                delete.deleteColumns(kv.getFamily(), kv.getQualifier());
                testTable.delete(tx, delete);
            }
        }
        tx.commit();
        scanner.close();

        testTable.close();
    }

    @Test
    public void testConflictAndAbort() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testConflictAndAbort.test");

        HaeinsaTransaction tx = tm.begin();
        HaeinsaTransaction tx2 = tm.begin();

        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        put2.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        testTable.put(tx, put);
        testTable.put(tx, put2);

        testTable.put(tx2, put);
        tx2.commit();
        try {
            tx.commit();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConflictException);
        }

        tx = tm.begin();
        HaeinsaScan scan = new HaeinsaScan();
        HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
        HaeinsaResult result = scanner.next();
        HaeinsaResult result2 = scanner.next();

        Assert.assertNull(result2);
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
        Assert.assertEquals(result.getRow(), Bytes.toBytes("ymkim"));
        scanner.close();
        tx.rollback();

        tx = tm.begin();
        put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put2 = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        put2.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx, put);
        testTable.put(tx, put2);

        tx.commit();

        tx = tm.begin();
        scan = new HaeinsaScan();
        scanner = testTable.getScanner(tx, scan);
        result = scanner.next();
        result2 = scanner.next();
        HaeinsaResult result3 = scanner.next();

        Assert.assertNull(result3);
        Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
        Assert.assertEquals(result.getRow(), Bytes.toBytes("kjwoo"));
        Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
        Assert.assertEquals(result2.getRow(), Bytes.toBytes("ymkim"));
        scanner.close();
        tx.rollback();

        tx = tm.begin();
        HaeinsaDelete delete1 = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete1.deleteFamily(Bytes.toBytes("data"));

        HaeinsaDelete delete2 = new HaeinsaDelete(Bytes.toBytes("kjwoo"));
        delete2.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));

        testTable.delete(tx, delete1);
        testTable.delete(tx, delete2);

        tx.commit();

        testTable.close();
    }

    @Test
    public void testConflictAndRecover() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testConflictAndRecover.test");
        final HaeinsaTableIfaceInternal testInternalTable = (HaeinsaTableIfaceInternal) testTable;

        HaeinsaTransaction tx1 = tm.begin();
        HaeinsaTransaction tx2 = tm.begin();

        HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put1.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        put2.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));

        testTable.put(tx1, put1);
        testTable.put(tx1, put2);
        testTable.put(tx2, put1);

        HaeinsaTableTransaction tableState = tx2.createOrGetTableState(Bytes.toBytes("HaeinsaUnitTest.testConflictAndRecover.test"));
        HaeinsaRowTransaction rowState = tableState.createOrGetRowState(Bytes.toBytes("ymkim"));

        // currentCommitTimestamp is System.currentTimeMillis(),
        // since this access is very first time of the row.
        // It was initially Long.MIN_VALUE but it become System.currentTimeMillis()
        long currentCommitTimestamp = System.currentTimeMillis();
        tx2.classifyAndSortRows(false);
        tx2.setPrewriteTimestamp(currentCommitTimestamp + 1);
        tx2.setCommitTimestamp(currentCommitTimestamp + 3);
        testInternalTable.prewrite(rowState, Bytes.toBytes("ymkim"), true);

        try {
            tx1.commit();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConflictException);
        }

        tx1 = tm.begin();
        try {
            HaeinsaScan scan = new HaeinsaScan();
            HaeinsaResultScanner scanner = testTable.getScanner(tx1, scan);
            HaeinsaResult result1 = scanner.next();
            HaeinsaResult result2 = scanner.next();

            Assert.assertNull(result2);
            Assert.assertEquals(result1.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
            Assert.assertEquals(result1.getRow(), Bytes.toBytes("ymkim"));
            scanner.close();
            tx1.rollback();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ConflictException);
        }

        Thread.sleep(HaeinsaConstants.ROW_LOCK_TIMEOUT + 100);

        tx1 = tm.begin();
        HaeinsaScan scan = new HaeinsaScan();
        try (HaeinsaResultScanner scanner = testTable.getScanner(tx1, scan)) {
            HaeinsaResult result = scanner.next();
            Assert.assertNull(result);
        }

        tx1 = tm.begin();
        scan = new HaeinsaScan();
        try (HaeinsaResultScanner scanner = testTable.getScanner(tx1, scan)) {
            HaeinsaResult result = scanner.next();
            Assert.assertNull(result);
        }

        put1 = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put1.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put2 = new HaeinsaPut(Bytes.toBytes("kjwoo"));
        put2.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
        testTable.put(tx1, put1);
        testTable.put(tx1, put2);

        tx1.commit();

        tx1 = tm.begin();
        scan = new HaeinsaScan();
        try (HaeinsaResultScanner scanner = testTable.getScanner(tx1, scan)) {
            HaeinsaResult result = scanner.next();
            HaeinsaResult result2 = scanner.next();
            HaeinsaResult result3 = scanner.next();

            Assert.assertNull(result3);
            Assert.assertEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
            Assert.assertEquals(result.getRow(), Bytes.toBytes("kjwoo"));
            Assert.assertEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
            Assert.assertEquals(result2.getRow(), Bytes.toBytes("ymkim"));
        }
        tx1.rollback();

        tx1 = tm.begin();
        HaeinsaDelete delete1 = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete1.deleteFamily(Bytes.toBytes("data"));

        HaeinsaDelete delete2 = new HaeinsaDelete(Bytes.toBytes("kjwoo"));
        delete2.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));

        testTable.delete(tx1, delete1);
        testTable.delete(tx1, delete2);

        tx1.commit();

        testTable.close();
    }

    /**
     * Unit test for multiple mutations for any rows in {@link HaeinsaTransaction}.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleMutations() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testMultipleMutations.test");

        /*
         * - beginTransaction
         * 1. put { row-abc, data, column-a }
         * 2. put { row-abc, data, column-b }
         * 3. put { row-d, meta, column-d }
         * - commit
         */
        HaeinsaTransaction tx = tm.begin();
        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row-abc"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("column-a"), Bytes.toBytes("value-a"));
        testTable.put(tx, put);

        put = new HaeinsaPut(Bytes.toBytes("row-abc"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("column-b"), Bytes.toBytes("value-b"));
        testTable.put(tx, put);

        put = new HaeinsaPut(Bytes.toBytes("row-d"));
        put.add(Bytes.toBytes("meta"), Bytes.toBytes("column-d"), Bytes.toBytes("value-d"));
        testTable.put(tx, put);

        tx.commit();

        /*
         * - beginTransaction
         * 4. put { row-abc, data, column-c }
         * 5. put { row-e, meta, column-e }
         * 6. deleteFamily { row-abc, data }
         * 7. put { row-abc, data, col-after }
         * - commit
         */
        tx = tm.begin();
        put = new HaeinsaPut(Bytes.toBytes("row-abc"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("column-c"), Bytes.toBytes("value-c"));
        testTable.put(tx, put);

        put = new HaeinsaPut(Bytes.toBytes("row-e"));
        put.add(Bytes.toBytes("meta"), Bytes.toBytes("column-e"), Bytes.toBytes("value-e"));
        testTable.put(tx, put);

        HaeinsaDelete delete = new HaeinsaDelete(Bytes.toBytes("row-abc"));
        delete.deleteFamily(Bytes.toBytes("data"));
        testTable.delete(tx, delete);

        put = new HaeinsaPut(Bytes.toBytes("row-abc"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-after"), Bytes.toBytes("value-after"));
        testTable.put(tx, put);

        tx.commit();

        /*
         * Check the result. There should be data added by 3, 5, 7 operations.
         * Other data was deleted.
         *
         * 3. put { row-d, meta, column-d }
         * 5. put { row-e, meta, column-e }
         * 7. put { row-abc, data, col-after } (There should be unique column for row-abc)
         */
        tx = tm.begin();
        HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row-d"));
        get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("column-d"));
        Assert.assertEquals(testTable.get(tx, get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("column-d")),
                Bytes.toBytes("value-d"));

        get = new HaeinsaGet(Bytes.toBytes("row-e"));
        get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("column-e"));
        Assert.assertEquals(testTable.get(tx, get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("column-e")),
                Bytes.toBytes("value-e"));

        get = new HaeinsaGet(Bytes.toBytes("row-abc"));
        HaeinsaResult result = testTable.get(tx, get);
        Assert.assertTrue(result.list().size() == 1);
        Assert.assertEquals(testTable.get(tx, get).getValue(Bytes.toBytes("data"), Bytes.toBytes("col-after")),
                Bytes.toBytes("value-after"));

        tx.rollback();

        testTable.close();
    }

    /**
     *
     * Unit test for check get/scan without transaction.
     *
     * @throws Exception
     */
    @Test
    public void testHaeinsaTableWithoutTx() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testHaeinsaTableWithoutTx.test");
        final HTableInterface hTestTable = CLUSTER.getHbaseTable("HaeinsaUnitTest.testHaeinsaTableWithoutTx.test");

        /*
         * - beginTransaction
         * 1. Put { row-put-a, data, col-put-a }
         * 2. Put { row-put-b, data, col-put-b }
         * - commit
         *
         * - beginTransaction
         * 3. GetWithoutTx { row-put-a, data }
         * 4. Put { row-put-b, data, col-put-b }
         * - commit
         *
         * Lock of row-put-a should not be changed, and lock of row-put-b should be changed.
         */
        // put initial data
        HaeinsaTransaction tx = tm.begin();
        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row-put-a"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-put-a"), Bytes.toBytes("value-put-a"));
        testTable.put(tx, put);
        put = new HaeinsaPut(Bytes.toBytes("row-put-b"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-put-b"), Bytes.toBytes("value-put-b"));
        testTable.put(tx, put);
        tx.commit();

        // getWithoutTx
        tx = tm.begin();
        byte[] row = Bytes.toBytes("row-put-a");
        byte[] oldLockGet = getLock(hTestTable, row);
        HaeinsaGet get = new HaeinsaGet(row);
        get.addFamily(Bytes.toBytes("data"));
        testTable.get(null, get); // getWithoutTx

        row = Bytes.toBytes("row-put-b");
        byte[] oldLockPut = getLock(hTestTable, row);
        put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-put-b"), Bytes.toBytes("value-put-b-new"));
        testTable.put(tx, put); // getWithoutTx

        tx.commit();

        // lock at { row-put-a } not changed
        row = Bytes.toBytes("row-put-a");
        Assert.assertFalse(checkLockChanged(hTestTable, row, oldLockGet));
        // lock at { row-put-b } changed
        row = Bytes.toBytes("row-put-b");
        Assert.assertTrue(checkLockChanged(hTestTable, row, oldLockPut));

        /*
         * - beginTransaction
         * 1. ScanWithtoutTx { row-put-a ~ row-put-c }
         * 2. Put { row-put-c, data, col-put-c }
         * - commit
         *
         * Lock of row-put-a and row-put-b should not be changed, and lock of row-put-c should be changed.
         */
        // getScannerWithoutTx ( HaeinsaScan )
        tx = tm.begin();
        row = Bytes.toBytes("row-put-a");
        byte[] oldLockScan1 = getLock(hTestTable, row);
        row = Bytes.toBytes("row-put-b");
        byte[] oldLockScan2 = getLock(hTestTable, row);
        HaeinsaScan scan = new HaeinsaScan();
        scan.setStartRow(Bytes.toBytes("row-put-a"));
        scan.setStopRow(Bytes.toBytes("row-put-c"));
        HaeinsaResultScanner resultScanner = testTable.getScanner(null, scan);
        resultScanner.next();
        resultScanner.next();
        resultScanner.close();

        row = Bytes.toBytes("row-put-c");
        oldLockPut = getLock(hTestTable, row);
        put = new HaeinsaPut(Bytes.toBytes("row-put-c"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-put-c"), Bytes.toBytes("value-put-c"));
        testTable.put(tx, put);
        tx.commit();

        // lock at { row-put-a } not changed
        row = Bytes.toBytes("row-put-a");
        Assert.assertFalse(checkLockChanged(hTestTable, row, oldLockScan1));
        // lock at { row-put-b } not changed
        row = Bytes.toBytes("row-put-b");
        Assert.assertFalse(checkLockChanged(hTestTable, row, oldLockScan2));
        // lock at { row-put-c } changed
        row = Bytes.toBytes("row-put-c");
        Assert.assertTrue(checkLockChanged(hTestTable, row, oldLockPut));

        /*
         * - beginTransaction
         * 1. Put { row-put-d, data, [ col-put-a, col-put-b, col-put-c ] }
         * - commit
         *
         * - beginTransaction
         * 2. IntraScanWithoutTx { row-put-d, data, [ col-put-a ~ col-put-d ] }
         * 3. Put { row-put-e, data, col-put-e }
         * - commit
         *
         * Lock of row-put-d should not be changed, and lock of row-put-e should be changed.
         */
        // getScannerWithoutTx ( HaeinsaIntrascan )
        tx = tm.begin();
        row = Bytes.toBytes("row-put-d");
        put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-put-a"), Bytes.toBytes("value-put-a"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-put-b"), Bytes.toBytes("value-put-b"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("col-put-c"), Bytes.toBytes("value-put-c"));
        tx.commit();

        tx = tm.begin();
        row = Bytes.toBytes("row-put-d");
        byte[] oldLockIntraScan = getLock(hTestTable, row);
        HaeinsaIntraScan intraScan = new HaeinsaIntraScan(
                row, Bytes.toBytes("col-put-a"), true, Bytes.toBytes("col-put-d"), true);
        intraScan.addFamily(Bytes.toBytes("data"));
        resultScanner = testTable.getScanner(null, intraScan);
        resultScanner.next();
        resultScanner.next();
        resultScanner.next();
        resultScanner.close();
        row = Bytes.toBytes("row-put-e");
        oldLockPut = getLock(hTestTable, row);
        testTable.put(tx,
                new HaeinsaPut(row).
                        add(Bytes.toBytes("data"), Bytes.toBytes("col-put-e"), Bytes.toBytes("value-put-e")));
        tx.commit();

        // lock at { row-put-d } not changed
        row = Bytes.toBytes("row-put-d");
        Assert.assertFalse(checkLockChanged(hTestTable, row, oldLockIntraScan));
        // lock at { row-put-e } changed
        row = Bytes.toBytes("row-put-e");
        Assert.assertTrue(checkLockChanged(hTestTable, row, oldLockPut));

        // release resources
        testTable.close();
        hTestTable.close();
    }

    @Test
    public void testDanglingRowLockException() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();
        final HaeinsaTableIface testTable = CLUSTER.getHaeinsaTable("HaeinsaUnitTest.testDanglingRowLockException.test");
        final HTableInterface hTestTable = CLUSTER.getHbaseTable("HaeinsaUnitTest.testDanglingRowLockException.test");

        {
            TRowKey primaryRowKey = new TRowKey().setTableName(testTable.getTableName()).setRow(Bytes.toBytes("James"));
            TRowKey danglingRowKey = new TRowKey().setTableName(testTable.getTableName()).setRow(Bytes.toBytes("Brad"));
            TRowLock danglingRowLock = new TRowLock(HaeinsaConstants.ROW_LOCK_VERSION, TRowLockState.PREWRITTEN, 1376526618707L)
                    .setCurrentTimestamp(1376526618705L)
                    .setExpiry(1376526623706L)
                    .setPrimary(primaryRowKey);

            Put hPut = new Put(danglingRowKey.getRow());
            hPut.add(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER,
                    danglingRowLock.getCurrentTimestamp(), TRowLocks.serialize(danglingRowLock));
            hTestTable.put(hPut);

            HaeinsaTransaction tx = tm.begin();
            HaeinsaPut put = new HaeinsaPut(danglingRowKey.getRow());
            put.add(Bytes.toBytes("data"), Bytes.toBytes("balance"), Bytes.toBytes(1000));
            try {
                testTable.put(tx, put);
                tx.commit();
                Assert.fail();
            } catch (DanglingRowLockException e) {
                Assert.assertEquals(e.getDanglingRowKey(), danglingRowKey);
            }
        }

        {
            TRowKey primaryRowKey = new TRowKey().setTableName(testTable.getTableName()).setRow(Bytes.toBytes("Andrew"));
            TRowLock primaryRowLock = new TRowLock(HaeinsaConstants.ROW_LOCK_VERSION, TRowLockState.STABLE, System.currentTimeMillis());
            TRowKey danglingRowKey = new TRowKey().setTableName(testTable.getTableName()).setRow(Bytes.toBytes("Alpaca"));
            TRowLock danglingRowLock = new TRowLock(HaeinsaConstants.ROW_LOCK_VERSION, TRowLockState.PREWRITTEN, 1376526618717L)
                    .setCurrentTimestamp(1376526618715L)
                    .setExpiry(1376526623716L)
                    .setPrimary(primaryRowKey);

            Put hPut = new Put(danglingRowKey.getRow());
            hPut.add(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER,
                    danglingRowLock.getCurrentTimestamp(), TRowLocks.serialize(danglingRowLock));
            hTestTable.put(hPut);

            hPut = new Put(primaryRowKey.getRow());
            hPut.add(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER,
                    primaryRowLock.getCommitTimestamp(), TRowLocks.serialize(primaryRowLock));
            hTestTable.put(hPut);

            HaeinsaTransaction tx = tm.begin();
            HaeinsaPut put = new HaeinsaPut(danglingRowKey.getRow());
            put.add(Bytes.toBytes("data"), Bytes.toBytes("balance"), Bytes.toBytes(1000));
            try {
                testTable.put(tx, put);
                tx.commit();
                Assert.fail();
            } catch (DanglingRowLockException e) {
                Assert.assertEquals(e.getDanglingRowKey(), danglingRowKey);
            }
        }

        // release resources
        testTable.close();
        hTestTable.close();
    }

}
