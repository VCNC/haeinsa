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

import org.testng.Assert;
import org.testng.annotations.Test;

public class HaeinsaTransactionLocalsTest extends HaeinsaTestBase {
    private static HaeinsaTransactionLocal<Integer> LOCAL = HaeinsaTransactionLocal.newLocal();

    @Test
    public void testBasicOperations() throws Exception {
        final HaeinsaTransactionManager tm = CLUSTER.getTransactionManager();

        HaeinsaTransaction tx1 = tm.begin();
        Assert.assertFalse(LOCAL.isSet(tx1));
        Assert.assertNull(LOCAL.get(tx1));
        LOCAL.set(tx1, 1);
        Assert.assertTrue(LOCAL.isSet(tx1));
        Assert.assertEquals(LOCAL.get(tx1), Integer.valueOf(1));
        LOCAL.remove(tx1);
        Assert.assertFalse(LOCAL.isSet(tx1));
        Assert.assertNull(LOCAL.get(tx1));

        HaeinsaTransaction tx2 = tm.begin();
        Assert.assertFalse(LOCAL.isSet(tx2));
        Assert.assertNull(LOCAL.get(tx2));
        LOCAL.set(tx2, 2);
        Assert.assertTrue(LOCAL.isSet(tx2));
        Assert.assertEquals(LOCAL.get(tx2), Integer.valueOf(2));
        LOCAL.remove(tx2);
        Assert.assertFalse(LOCAL.isSet(tx2));
        Assert.assertNull(LOCAL.get(tx2));
    }
}
