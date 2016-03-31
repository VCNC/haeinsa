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

import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HaeinsaRowTransactionTest {

    @Test
    public void testFilterColumns() {
        byte[] row = Bytes.toBytes("ymkim");
        HaeinsaDelete delete = new HaeinsaDelete(row);
        delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));

        HaeinsaPut put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        HaeinsaRowTransaction.FilterResult filterResult = HaeinsaRowTransaction.filter(new HaeinsaDeleteTracker(delete), put);
        Assert.assertFalse(filterResult.getDeleted().isEmpty());
        Assert.assertFalse(filterResult.getRemained().isEmpty());

        HaeinsaPut expectedDeleted = new HaeinsaPut(row);
        expectedDeleted.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        Assert.assertEquals(filterResult.getDeleted().toTMutation(), expectedDeleted.toTMutation());

        HaeinsaPut expectedRemained = new HaeinsaPut(row);
        expectedRemained.add(Bytes.toBytes("data"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        Assert.assertEquals(filterResult.getRemained().toTMutation(), expectedRemained.toTMutation());
    }

    @Test
    public void testFilterFamily() {
        byte[] row = Bytes.toBytes("ymkim");
        HaeinsaDelete delete = new HaeinsaDelete(row);
        delete.deleteFamily(Bytes.toBytes("data"));

        HaeinsaPut put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        HaeinsaRowTransaction.FilterResult filterResult = HaeinsaRowTransaction.filter(new HaeinsaDeleteTracker(delete), put);
        Assert.assertFalse(filterResult.getDeleted().isEmpty());
        Assert.assertFalse(filterResult.getRemained().isEmpty());

        HaeinsaPut expectedDeleted = new HaeinsaPut(row);
        expectedDeleted.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        Assert.assertEquals(filterResult.getDeleted().toTMutation(), expectedDeleted.toTMutation());

        HaeinsaPut expectedRemained = new HaeinsaPut(row);
        expectedRemained.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        Assert.assertEquals(filterResult.getRemained().toTMutation(), expectedRemained.toTMutation());
    }

    @Test
    public void testMerge() throws Exception {
        byte[] row = Bytes.toBytes("ymkim");
        HaeinsaRowTransaction.MutationMerger merger = new HaeinsaRowTransaction.MutationMerger(row);

        HaeinsaPut put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        merger.merge(put);

        Assert.assertTrue(merger.getFirstDelete().isEmpty());
        Assert.assertEquals(merger.getLastPut().toTMutation(), put.toTMutation());

        HaeinsaDelete delete = new HaeinsaDelete(row);
        delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));

        merger.merge(delete);

        HaeinsaPut expectedPut = new HaeinsaPut(row);
        expectedPut.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        Assert.assertEquals(merger.getFirstDelete().toTMutation(), delete.toTMutation());
        Assert.assertEquals(merger.getLastPut().toTMutation(), expectedPut.toTMutation());
        Assert.assertTrue(merger.canExchangeDeleteAndPut());

        put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put.add(Bytes.toBytes("data2"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        merger.merge(put);

        expectedPut = new HaeinsaPut(row);
        expectedPut.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        expectedPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        expectedPut.add(Bytes.toBytes("data2"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        Assert.assertTrue(merger.getFirstDelete().isEmpty());
        Assert.assertEquals(merger.getLastPut().toTMutation(), expectedPut.toTMutation());

        delete = new HaeinsaDelete(row);
        delete.deleteFamily(Bytes.toBytes("data1"));
        merger.merge(delete);

        expectedPut = new HaeinsaPut(row);
        expectedPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        expectedPut.add(Bytes.toBytes("data2"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        Assert.assertEquals(merger.getLastPut().toTMutation(), expectedPut.toTMutation());
        Assert.assertEquals(merger.getFirstDelete().toTMutation(), delete.toTMutation());
        Assert.assertTrue(merger.canExchangeDeleteAndPut());

        put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        merger.merge(put);

        expectedPut = new HaeinsaPut(row);
        expectedPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        expectedPut.add(Bytes.toBytes("data2"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        expectedPut.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        Assert.assertEquals(merger.getFirstDelete().toTMutation(), delete.toTMutation());
        Assert.assertEquals(merger.getLastPut().toTMutation(), expectedPut.toTMutation());
        Assert.assertFalse(merger.canExchangeDeleteAndPut());

        put = new HaeinsaPut(row);
        put.add(Bytes.toBytes("data1"), Bytes.toBytes("lastName"), Bytes.toBytes("Kim"));
        put.add(Bytes.toBytes("data3"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        merger.merge(put);

        expectedPut = new HaeinsaPut(row);
        expectedPut.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        expectedPut.add(Bytes.toBytes("data2"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        expectedPut.add(Bytes.toBytes("data3"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        expectedPut.add(Bytes.toBytes("data1"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        expectedPut.add(Bytes.toBytes("data1"), Bytes.toBytes("lastName"), Bytes.toBytes("Kim"));

        Assert.assertEquals(merger.getFirstDelete().toTMutation(), delete.toTMutation());
        Assert.assertEquals(merger.getLastPut().toTMutation(), expectedPut.toTMutation());
        Assert.assertFalse(merger.canExchangeDeleteAndPut());
    }
}
