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

import kr.co.vcnc.haeinsa.thrift.TRowLocks;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class HaeinsaTransactionTest extends HaeinsaTestBase {

    @Test
    public void testHasChanges() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface table = context().getHaeinsaTableIface("test");

        // Tests read-only transaction
        {
            HaeinsaTransaction tx = tm.begin();
            Assert.assertFalse(tx.hasChanges());

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qualifier"));
            table.get(tx, get1);
            Assert.assertFalse(tx.hasChanges());

            HaeinsaGet get2 = new HaeinsaGet(Bytes.toBytes("row2"));
            get2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qualifier"));
            table.get(tx, get2);
            Assert.assertFalse(tx.hasChanges());
        }

        // Tests single-row put only transaction
        {
            HaeinsaTransaction tx = tm.begin();
            Assert.assertFalse(tx.hasChanges());

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qualifier"));
            table.get(tx, get1);
            Assert.assertFalse(tx.hasChanges());

            HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("row1"));
            put1.add(Bytes.toBytes("data"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
            table.put(tx, put1);
            Assert.assertTrue(tx.hasChanges());
        }

        // Tests multi-row mutation transaction
        {
            HaeinsaTransaction tx = tm.begin();
            Assert.assertFalse(tx.hasChanges());

            HaeinsaGet get1 = new HaeinsaGet(Bytes.toBytes("row1"));
            get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qualifier"));
            table.get(tx, get1);
            Assert.assertFalse(tx.hasChanges());

            HaeinsaDelete delete1 = new HaeinsaDelete(Bytes.toBytes("row1"));
            delete1.deleteFamily(Bytes.toBytes("data"));
            table.delete(tx, delete1);
            Assert.assertTrue(tx.hasChanges());

            HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("row2"));
            put1.add(Bytes.toBytes("data"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
            table.put(tx, put1);
            Assert.assertTrue(tx.hasChanges());
        }
    }

    @Test
    public void testTimeout() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface table = context().getHaeinsaTableIface("test");
        final HTableInterface htable = context().getHTableInterface("test");
        final HaeinsaTableIfaceInternal interalTable = (HaeinsaTableIfaceInternal) table;

        // Tests created, timeout and expiry fields
        {
            HaeinsaTransaction tx = tm.begin();
            Assert.assertEquals(tx.getCreated(), System.currentTimeMillis());
            Assert.assertEquals(tx.getExpiry(), tx.getCreated() + tx.getTimeout());

            long originalExpiry = tx.getExpiry();
            tx.setTimeout(TimeUnit.SECONDS.toMillis(20));
            Assert.assertEquals(tx.getTimeout(), TimeUnit.SECONDS.toMillis(20));
            Assert.assertEquals(tx.getExpiry(), tx.getCreated() + tx.getTimeout());
            Assert.assertNotEquals(tx.getExpiry(), originalExpiry);

            tx.rollback();
        }
        // Tests expiry in HBase
        {
            HaeinsaTransaction tx = tm.begin();
            tx.setTimeout(TimeUnit.SECONDS.toMillis(20));

            HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("brad"));
            put1.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("+821411111111"));
            table.put(tx, put1);

            HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("james"));
            put2.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("+821422222222"));
            table.put(tx, put2);

            // Simulate prewrite of the transaction
            HaeinsaTableTransaction tableState = tx.createOrGetTableState(table.getTableName());
            HaeinsaRowTransaction rowState = tableState.createOrGetRowState(Bytes.toBytes("brad"));

            long currentCommitTimestamp = System.currentTimeMillis();
            tx.classifyAndSortRows(false);
            tx.setPrewriteTimestamp(currentCommitTimestamp + 1);
            tx.setCommitTimestamp(currentCommitTimestamp + 3);
            interalTable.prewrite(rowState, Bytes.toBytes("brad"), true);

            // Check whether expiry in HBase is same as expiry of HaeinsaTransaction
            Get hGet = new Get(Bytes.toBytes("brad"));
            hGet.addColumn(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER);
            Result primaryResult = htable.get(hGet);
            TRowLock rowLock = TRowLocks.deserialize(primaryResult.getValue(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER));
            Assert.assertEquals(rowLock.getState(), TRowLockState.PREWRITTEN);
            Assert.assertEquals(rowLock.getCommitTimestamp(), tx.getCommitTimestamp());
            Assert.assertEquals(rowLock.getExpiry(), tx.getExpiry());
        }
    }
}
