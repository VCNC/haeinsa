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
import org.junit.Ignore;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;


public class HaeinsaTransactionMultiThreadTest extends HaeinsaTestBase {
    @Ignore//This test fails. as an example for the reason we need the threadSafe transaction
    @Test
    public void testChangesAsExpected() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface table = context().getHaeinsaTableIface("test");
        int conccurency = 10;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        FutureTask<Void>[] tasks = new FutureTask[conccurency];
        // Tests multi-row mutation transaction
        {
            final HaeinsaTransaction tx = tm.begin();
            Assert.assertFalse(tx.hasChanges());
            for (int i = 0; i < conccurency; i++){
                final String callId = i + "";
                tasks[i] = new FutureTask<Void>(new Callable<Void>(){
                    HaeinsaTransaction trx = tx;
                    String id = callId;
                    final HaeinsaTableIface table = trx.getManager().getTablePool().getTable(context().createContextedTableName("test"));

                    @Override
                    public Void call() throws Exception {
                        HaeinsaDelete delete1 = new HaeinsaDelete(Bytes.toBytes("row1" + id));
                        delete1.deleteFamily(Bytes.toBytes("data"));
                        table.delete(tx, delete1);

                        HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("row2" + id));
                        put1.add(Bytes.toBytes("data"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
                        table.put(tx, put1);
                        table.close();
                        return null;
                    }
                });
            }
            for (int i = 0; i < conccurency; i++){
                //running all tasks
                executor.execute(tasks[i]);
            }
            for (int i = 0; i < conccurency; i++){
                //waiting for all to finish
                tasks[i].get();
            }
            tx.classifyAndSortRows(true);
            Assert.assertEquals(tx.getMutationRowStates().size(), conccurency * 2);
        }
    }

    @Test
    public void testChangesAsExpected2() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTransactionManager threadSafetm = new HaeinsaTransactionManager(context().getTransactionManager().getTablePool(), true);
        final HaeinsaTableIface table = context().getHaeinsaTableIface("test");
        int conccurency = 10;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        FutureTask<Void>[] tasks = new FutureTask[conccurency];
        // Tests multi-row mutation transaction
        {
            final HaeinsaTransaction tx = threadSafetm.begin();
            Assert.assertFalse(tx.hasChanges());
            for (int i = 0; i < conccurency; i++){
                final String callId = i + "";
                tasks[i] = new FutureTask<Void>(new Callable<Void>(){
                    HaeinsaTransaction trx = tx;
                    String id = callId;
                    final HaeinsaTableIface table = trx.getManager().getTablePool().getTable(context().createContextedTableName("test"));

                    @Override
                    public Void call() throws Exception {
                        HaeinsaDelete delete1 = new HaeinsaDelete(Bytes.toBytes("row1" + id));
                        delete1.deleteFamily(Bytes.toBytes("data"));
                        table.delete(tx, delete1);

                        HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes("row2" + id));
                        put1.add(Bytes.toBytes("data"), Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
                        table.put(tx, put1);
                        table.close();
                        return null;
                    }
                });
            }
            for (int i = 0; i < conccurency; i++){
                //running all tasks
                executor.execute(tasks[i]);
            }
            for (int i = 0; i < conccurency; i++){
                //waiting for all to finish
                tasks[i].get();
            }
            tx.classifyAndSortRows(true);
            Assert.assertEquals(tx.getMutationRowStates().size(), conccurency * 2);
        }
    }

    @Test
    public void testProtectWritingToTheSameRow() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTransactionManager threadSafetm = new HaeinsaTransactionManager(context().getTransactionManager().getTablePool(), true);
        final HaeinsaTableIface table = context().getHaeinsaTableIface("test");
        final String key = "key";
        final Random random = new Random();
        int conccurency = 4;
        ExecutorService executor = Executors.newFixedThreadPool(conccurency);
        FutureTask<Void>[] tasks = new FutureTask[conccurency];
        {
            for (int j = 0; j < 5; j++) {
                final HaeinsaTransaction tx = threadSafetm.begin();
                for (int i = 0; i < conccurency; i++) {
                    tasks[i] = new FutureTask<Void>(new Callable<Void>() {
                        HaeinsaTransaction trx = tx;
                        final HaeinsaTableIface table = trx.getManager().getTablePool().getTable(context().createContextedTableName("test"));

                        @Override
                        public Void call() throws Exception {
                            Thread.sleep(random.nextInt(200));
                            long counter = 1;
                            HaeinsaGet get = new HaeinsaGet(Bytes.toBytes(key));
                            HaeinsaResult result = table.get(tx, get);
                            byte[] dataAsBytes = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("qualifier"));
                            if (dataAsBytes != null) {
                                counter = Bytes.toLong(dataAsBytes) + 1;
                            }
                            try {
                                HaeinsaPut put1 = new HaeinsaPut(Bytes.toBytes(key));
                                put1.add(Bytes.toBytes("data"), Bytes.toBytes("qualifier"), Bytes.toBytes(counter));
                                table.put(tx, put1);
                            } catch (IllegalStateException e) {
                                Assert.assertTrue(e.getMessage().contains("This row was already changed."));
                            }

                            table.close();
                            return null;
                        }
                    });
                }
                for (int i = 0; i < conccurency; i++) {
                    //running all tasks
                    executor.execute(tasks[i]);
                }
                for (int i = 0; i < conccurency; i++) {
                    //waiting for all to finish
                    tasks[i].get();
                }
                tx.classifyAndSortRows(true);
                Assert.assertEquals(tx.getMutationRowStates().size(), 1);
                tx.commit();
            }

            HaeinsaTransaction regularTx = tm.begin();

            HaeinsaGet get = new HaeinsaGet(Bytes.toBytes(key));

            HaeinsaResult result = table.get(regularTx, get);
            byte[] dataAsBytes = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("qualifier"));
            Assert.assertEquals(5, Bytes.toLong(dataAsBytes));
            table.close();
        }
    }

}
