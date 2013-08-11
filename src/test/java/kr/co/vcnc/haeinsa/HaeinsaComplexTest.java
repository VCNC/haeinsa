/**
 * Copyright (C) 2013 VCNC, inc
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Complex multi-thread unit test for Haeinsa. It contains simple-increment
 * test, concurrent random increment test, and serializability test.
 */
public class HaeinsaComplexTest {
	private static MiniHBaseCluster CLUSTER;
	private static Configuration CONF;

	@BeforeClass
	public static void setUpHbase() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseTestingUtility utility = new HBaseTestingUtility(conf);
		utility.cleanupTestDir();
		CLUSTER = utility.startMiniCluster();
		CONF = CLUSTER.getConfiguration();
		HBaseAdmin admin = new HBaseAdmin(CONF);

		// Table -> ColumnFamily
		// { test } -> { !lock!, data }
		HTableDescriptor tableDesc = new HTableDescriptor("test");
		HColumnDescriptor lockColumnDesc = new HColumnDescriptor(HaeinsaConstants.LOCK_FAMILY);
		lockColumnDesc.setMaxVersions(1);
		lockColumnDesc.setInMemory(true);
		tableDesc.addFamily(lockColumnDesc);
		HColumnDescriptor dataColumnDesc = new HColumnDescriptor("data");
		tableDesc.addFamily(dataColumnDesc);
		admin.createTable(tableDesc);

		admin.close();
	}

	@AfterClass
	public static void tearDownHBase() throws Exception {
		CLUSTER.shutdown();
	}

	/**
	 * 1 개의 Thread 에서 DB 의 특정 row 에 1 씩 증가하는 Transaction 을 걸고, 여러 차례의
	 * Transaction 후에 결과가 예상한 대로 나오는지 테스트한다.
	 *
	 * @throws Exception
	 */
	@Test
	public void testSimepleIncrement() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		final HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableIface testTable = tablePool.getTable("test");
		HaeinsaTransaction tx;

		final AtomicLong count = new AtomicLong(0);
		final long maxIter = 1000;

		// initial value
		final byte[] row = Bytes.toBytes("count");
		final byte[] CF = Bytes.toBytes("data");
		final byte[] CQ = Bytes.toBytes("value");

		tx = tm.begin();
		HaeinsaPut put = new HaeinsaPut(row);
		put.add(CF, CQ, Bytes.toBytes(0L));
		testTable.put(tx, put);
		tx.commit();

		for (int i = 0; i < maxIter; i++) {
			try {
				tx = tm.begin();
				HaeinsaGet get = new HaeinsaGet(row);
				get.addColumn(CF, CQ);
				long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));
				put = new HaeinsaPut(row);
				countOnDB += 1;
				put.add(CF, CQ, Bytes.toBytes(countOnDB));
				testTable.put(tx, put);
				tx.commit();
				count.addAndGet(1L);
			} catch (IOException e) {
				// IOException on HBase operations
			}
		}

		// check result
		tx = tm.begin();
		HaeinsaGet get = new HaeinsaGet(row);
		get.addColumn(CF, CQ);
		long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));
		tx.rollback();

		assertEquals(countOnDB, maxIter);
		assertEquals(count.get(), maxIter);

		testTable.close();
		tablePool.close();
		threadPool.shutdown();
	}

	/**
	 * 동시에 numberOfJob 개의 Thread 가 동일한 row 에 접근해서 random 하게 값을 증가시키는 transaction
	 * 을 시도한다. 여러 차례의 Transaction 이후에 local AtomicLong 에 저장한 값과 비교해서 일치하는 지
	 * 테스트한다.
	 *
	 * @throws Exception
	 */
	@Test
	public void testConcurrentRandomIncrement() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		final HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		final HaeinsaTableIface testTable = tablePool.getTable("test");
		HaeinsaTransaction tx;

		final AtomicLong count = new AtomicLong(0);
		final long maxIter = 100;
		final int randomRange = 100;
		int numberOfJob = 10;
		final AtomicLong successCount = new AtomicLong(0);
		final AtomicLong failCount = new AtomicLong(0);

		// initial value
		final byte[] row = Bytes.toBytes("count");
		final byte[] CF = Bytes.toBytes("data");
		final byte[] CQ = Bytes.toBytes("value");

		tx = tm.begin();
		HaeinsaPut put = new HaeinsaPut(row);
		put.add(CF, CQ, Bytes.toBytes(0L));
		testTable.put(tx, put);
		tx.commit();

		Runnable singleIncrementJob = new Runnable() {

			@Override
			public void run() {
				int iteration = 0;
				while (iteration < maxIter) {
					try {
						HaeinsaTransaction tx = tm.begin();
						HaeinsaGet get = new HaeinsaGet(row);
						get.addColumn(CF, CQ);
						long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));

						long newIncrement = new Random().nextInt(randomRange);
						countOnDB += newIncrement;

						HaeinsaPut put = new HaeinsaPut(row);
						put.add(CF, CQ, Bytes.toBytes(countOnDB));
						testTable.put(tx, put);
						tx.commit();
						count.addAndGet(newIncrement);
						iteration++;
						successCount.getAndIncrement();
					} catch (IOException e) {
						failCount.getAndIncrement();
					}
				}
				System.out.println(String.format("iteration : %d on Thread : %s", iteration, Thread.currentThread().getName()));
			}
		};

		ExecutorService service = Executors.newFixedThreadPool(numberOfJob,
				new ThreadFactoryBuilder().setNameFormat("IncrementJobThread-%d").build());

		for (int i = 0; i < numberOfJob; i++) {
			service.execute(singleIncrementJob);
		}

		Thread.sleep(10000);
		// check result
		tx = tm.begin();
		HaeinsaGet get = new HaeinsaGet(row);
		get.addColumn(CF, CQ);
		long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));
		tx.rollback();
		System.out.println(countOnDB);

		assertEquals(countOnDB, count.get());
		System.out.println("Number of Success Transactions : " + successCount.get());
		System.out.println("Number of Failed Transactions : " + failCount.get());
		System.out.println("Conflict rate : " + failCount.get() /
				((double) failCount.get() + (double) successCount.get()) * 100.0);

		// release resources
		testTable.close();
		tablePool.close();
		threadPool.shutdown();
		service.shutdown();
	}

	/**
	 * Serializability 를 테스트한다. row1 과 row2 에 각각 랜덤한 값을 쓰고 시작한다. 그 이후의 여러 개의
	 * Thread 에서 동시에 여러 번의 동일한 Transaction 을 시도한다. 각각의 Transaction 은 이전에 DB 에
	 * 있었던 값을 읽고 그 값을 기반으로 난수와 더한 후에 hash 를 걸어서 얻은 새로운 값을 DB 에 쓰게 된다.
	 *
	 * Pseudo code 는 다음과 같다.
	 * <pre>
	 * db.put (row1, hash( db.get(row1) + random ))
	 * db.put (row2, hash( db.get(row2) + random ))
	 * </pre>
	 *
	 * 만약 Transaction 이 성공을 하게 되면 Local memory lock 을 획득한 후에 Local 에 있는
	 * AtomicInteger 2개를 compareAndSet 으로 변경한다.
	 *
	 * 여러 횟수의 concurrent 한 Transaction 이후에 local memory 에 저장된 값과 DB 에 저장된 값이 같으면
	 * 해당 Schedule 은 Serializable 한 것이라고 볼 수 있다.
	 *
	 * @throws Exception
	 */
	@Test
	public void testSerializability() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		final HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		final HaeinsaTableIface testTable = tablePool.getTable("test");
		HaeinsaTransaction tx;

		// some random initial value
		final Object lock = new Object();
		final AtomicLong value1 = new AtomicLong(new Random().nextInt());
		final AtomicLong value2 = new AtomicLong(new Random().nextInt());

		final long maxIter = 100;
		final int numberOfJob = 10;
		final CountDownLatch countDownLatch = new CountDownLatch(numberOfJob);
		final AtomicLong successCount = new AtomicLong(0);
		final AtomicLong failCount = new AtomicLong(0);

		// initial value
		final byte[] CF = Bytes.toBytes("data");
		final byte[] row1 = Bytes.toBytes("row1");
		final byte[] CQ1 = Bytes.toBytes("col1");
		final byte[] row2 = Bytes.toBytes("row2");
		final byte[] CQ2 = Bytes.toBytes("col2");

		System.out.println("Start testSerializability test");
		tx = tm.begin();
		HaeinsaPut put = new HaeinsaPut(row1).add(CF, CQ1, Bytes.toBytes(value1.get()));
		testTable.put(tx, put);
		put = new HaeinsaPut(row2).add(CF, CQ2, Bytes.toBytes(value2.get()));
		testTable.put(tx, put);
		tx.commit();

		/*
		 * newValue1 = hashWithRandom( oldValue1 )
		 * newValue2 = hashWithRandom( oldValue2 )
		 *
		 * tx.begin();
		 * tx.write(newValue1);
		 * tx.write(newValue2);
		 * tx.commit();
		 */
		Runnable serialJob = new Runnable() {
			@Override
			public void run() {
				int iteration = 0;
				while (iteration < maxIter) {
					try {
						HaeinsaTransaction tx = tm.begin();
						long oldValue1 = Bytes.toLong(testTable.get(tx,
								new HaeinsaGet(row1).addColumn(CF, CQ1)).getValue(CF, CQ1));
						long oldValue2 = Bytes.toLong(testTable.get(tx,
								new HaeinsaGet(row2).addColumn(CF, CQ2)).getValue(CF, CQ2));
						long newValue1 = nextHashedValue(oldValue1);
						long newValue2 = nextHashedValue(oldValue2);

						testTable.put(tx, new HaeinsaPut(row1).add(CF, CQ1, Bytes.toBytes(newValue1)));
						testTable.put(tx, new HaeinsaPut(row2).add(CF, CQ2, Bytes.toBytes(newValue2)));

						tx.commit();

						// success
						iteration++;
						successCount.incrementAndGet();
						synchronized (lock) {
							assertTrue(value1.compareAndSet(oldValue1, newValue1));
							assertTrue(value2.compareAndSet(oldValue2, newValue2));
						}
					} catch (Exception e) {
						// fail
						failCount.getAndIncrement();
					}
				}
				System.out.println(String.format("iteration : %d on Thread : Ts", iteration, Thread.currentThread().getName()));
				countDownLatch.countDown();
			}
		};

		ExecutorService service = Executors.newFixedThreadPool(numberOfJob,
				new ThreadFactoryBuilder().setNameFormat("Serializability-job-thread-%d").build());

		for (int i = 0; i < numberOfJob; i++) {
			service.execute(serialJob);
		}
		countDownLatch.await();

		long dbValue1 = Bytes.toLong(testTable.get(tx, new HaeinsaGet(row1).addColumn(CF, CQ1)).getValue(CF, CQ1));
		long dbValue2 = Bytes.toLong(testTable.get(tx, new HaeinsaGet(row2).addColumn(CF, CQ2)).getValue(CF, CQ2));
		assertEquals(dbValue1, value1.get());
		assertEquals(dbValue2, value2.get());
		System.out.println("Number of Success Transactions : " + successCount.get());
		System.out.println("Number of Failed Transactions : " + failCount.get());
		System.out.println("Conflict rate : " + failCount.get() / ((double) failCount.get() + (double) successCount.get()) * 100.0);

		// release resources
		testTable.close();
		tablePool.close();
		threadPool.shutdown();
		service.shutdown();
	}

	/**
	 * return (str(oldValue) + str(random int)).hashCode()
	 *
	 * @param oldValue
	 * @return
	 */
	private long nextHashedValue(long oldValue) {
		String result = "";
		result += oldValue;
		result += new Random().nextInt();
		return result.hashCode();
	}
}
