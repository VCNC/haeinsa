/**
 * Copyright (C) 2013-2015 VCNC Inc.
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
import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * https://issues.vcnc.co.kr/browse/HAEINSA-67 When Haeinsa recovers rows, we
 * should make stable rows into new committimestamp's stable rows. This method
 * prevents making dangling row locks of long running transactions.
 */
public class Haeinsa67BugTest extends HaeinsaTestBase {

    @Test
    public void testRecover() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface testTable = context().getHaeinsaTableIface("test");
        final HTableInterface hTestTable = context().getHTableInterface("test");

        {
            TRowKey primaryRowKey = new TRowKey().setTableName(testTable.getTableName()).setRow(Bytes.toBytes("Andrew"));
            TRowKey secondaryRowKey = new TRowKey().setTableName(testTable.getTableName()).setRow(Bytes.toBytes("Brad"));
            TRowLock secondaryRowLock = new TRowLock(HaeinsaConstants.ROW_LOCK_VERSION, TRowLockState.STABLE, 1380504156137L);
            TRowLock primaryRowLock = new TRowLock(HaeinsaConstants.ROW_LOCK_VERSION, TRowLockState.PREWRITTEN, 1380504157100L)
                    .setCurrentTimestamp(1380504156000L)
                    .setExpiry(1380504160000L);
            primaryRowLock.addToSecondaries(secondaryRowKey);
            Put primaryPut = new Put(primaryRowKey.getRow());
            primaryPut.add(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER,
                    primaryRowLock.getCurrentTimestamp(), TRowLocks.serialize(primaryRowLock));
            hTestTable.put(primaryPut);

            Put secondaryPut = new Put(secondaryRowKey.getRow());
            secondaryPut.add(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER,
                    secondaryRowLock.getCommitTimestamp(), TRowLocks.serialize(secondaryRowLock));
            hTestTable.put(secondaryPut);

            HaeinsaTransaction tx = tm.begin();
            HaeinsaGet get = new HaeinsaGet(primaryRowKey.getRow());
            HaeinsaResult result = testTable.get(tx, get);
            Assert.assertTrue(result.isEmpty());

            Get hPrimaryGet = new Get(primaryRowKey.getRow());
            hPrimaryGet.addColumn(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER);
            Result primaryResult = hTestTable.get(hPrimaryGet);
            TRowLock stablePrimaryRowLock = TRowLocks.deserialize(primaryResult.getValue(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER));
            Assert.assertEquals(stablePrimaryRowLock.getState(), TRowLockState.STABLE);
            Assert.assertEquals(stablePrimaryRowLock.getCommitTimestamp(), primaryRowLock.getCommitTimestamp());

            Get hSecondaryGet = new Get(secondaryRowKey.getRow());
            hSecondaryGet.addColumn(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER);
            Result secondaryResult = hTestTable.get(hSecondaryGet);
            TRowLock stableSecondaryRowLock = TRowLocks.deserialize(secondaryResult.getValue(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER));
            Assert.assertEquals(stableSecondaryRowLock.getState(), TRowLockState.STABLE);
            Assert.assertEquals(stableSecondaryRowLock.getCommitTimestamp(), primaryRowLock.getCommitTimestamp());
        }

        testTable.close();
        hTestTable.close();
    }
}
