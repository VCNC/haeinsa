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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.testng.annotations.Test;

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
}
