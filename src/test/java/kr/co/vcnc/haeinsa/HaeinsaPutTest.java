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

public class HaeinsaPutTest {
    @Test
    public void testRemove() throws Exception {
        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        put.add(Bytes.toBytes("data2"), Bytes.toBytes("name"), Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data2"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        HaeinsaDelete delete = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete.deleteFamily(Bytes.toBytes("data2"));
        delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));

        put.remove(delete);

        HaeinsaPut expectedPut = new HaeinsaPut(Bytes.toBytes("ymkim"));
        expectedPut.add(Bytes.toBytes("data"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));

        Assert.assertEquals(put.toTMutation(), expectedPut.toTMutation());
    }
}
