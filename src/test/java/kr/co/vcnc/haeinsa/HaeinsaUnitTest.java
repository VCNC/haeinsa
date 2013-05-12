package kr.co.vcnc.haeinsa;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kr.co.vcnc.haeinsa.exception.ConflictException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Basic unit test for Haeinsa which consist of basic transaction test, multiple
 * mutations test, conflict and recover test, conflict and abort test,
 * HaeinsaWithoutTx test, and HBase migration test.
 */
public class HaeinsaUnitTest {
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
		// { test } -> { !lock!, data, meta }
		HTableDescriptor tableDesc = new HTableDescriptor("test");
		HColumnDescriptor lockColumnDesc = new HColumnDescriptor(HaeinsaConstants.LOCK_FAMILY);
		lockColumnDesc.setMaxVersions(1);
		lockColumnDesc.setInMemory(true);
		tableDesc.addFamily(lockColumnDesc);
		HColumnDescriptor dataColumnDesc = new HColumnDescriptor("data");
		tableDesc.addFamily(dataColumnDesc);
		HColumnDescriptor metaColumnDesc = new HColumnDescriptor("meta");
		tableDesc.addFamily(metaColumnDesc);
		admin.createTable(tableDesc);

		// Table -> ColumnFamily
		// { log } -> { !lock!, raw }
		HTableDescriptor logDesc = new HTableDescriptor("log");
		lockColumnDesc = new HColumnDescriptor(HaeinsaConstants.LOCK_FAMILY);
		lockColumnDesc.setMaxVersions(1);
		lockColumnDesc.setInMemory(true);
		logDesc.addFamily(lockColumnDesc);
		HColumnDescriptor rawColumnDesc = new HColumnDescriptor("raw");
		logDesc.addFamily(rawColumnDesc);
		admin.createTable(logDesc);

		admin.close();
	}

	@AfterClass
	public static void tearDownHBase() throws Exception {
		CLUSTER.shutdown();
	}

	@Test
	public void testTransaction() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableIface testTable = tablePool.getTable("test");

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
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

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

		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

		tx = tm.begin();
		HaeinsaScan scan = new HaeinsaScan();
		HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
		result = scanner.next();
		result2 = scanner.next();
		HaeinsaResult result3 = scanner.next();

		assertNull(result3);
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
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

		assertNull(result3);
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
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
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
		assertNull(result2);
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

		assertNull(result2);
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
		scanner.close();

		tx.commit();

		tx = tm.begin();
		get = new HaeinsaGet(Bytes.toBytes("ymkim"));
		result = testTable.get(tx, get);
		get2 = new HaeinsaGet(Bytes.toBytes("kjwoo"));
		result2 = testTable.get(tx, get2);
		tx.rollback();

		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

		assertTrue(result.isEmpty());
		assertFalse(result2.isEmpty());

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

		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));

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
		HaeinsaTableIface logTable = tablePool.getTable("log");
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
		assertArrayEquals(logTable.get(tx, get).getValue(Bytes.toBytes("raw"), Bytes.toBytes("time-0")), Bytes.toBytes("log-value-1"));
		get = new HaeinsaGet(Bytes.toBytes("row-0"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("time-0"));
		assertArrayEquals(testTable.get(tx, get).getValue(Bytes.toBytes("data"), Bytes.toBytes("time-0")), Bytes.toBytes("data-value-1"));
		get = new HaeinsaGet(Bytes.toBytes("row-0"));
		get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("time-0"));
		assertArrayEquals(testTable.get(tx, get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("time-0")), Bytes.toBytes("meta-value-1"));
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
				assertFalse(Bytes.equals(kv.getFamily(), HaeinsaConstants.LOCK_FAMILY));
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
				assertFalse(Bytes.equals(kv.getFamily(), HaeinsaConstants.LOCK_FAMILY));
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
		assertFalse(iter.hasNext());
		tx.rollback();
		scanner.close();
		// check whether table is clear - logTable
		tx = tm.begin();
		scan = new HaeinsaScan();
		scanner = logTable.getScanner(tx, scan);
		iter = scanner.iterator();
		assertFalse(iter.hasNext());
		tx.rollback();
		scanner.close();

		testTable.close();
		logTable.close();
		tablePool.close();
	}

	@Test
	public void testConflictAndAbort() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableIface testTable = tablePool.getTable("test");
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
			assertTrue(false);
		} catch (Exception e) {
			assertTrue(e instanceof ConflictException);
		}

		tx = tm.begin();
		HaeinsaScan scan = new HaeinsaScan();
		HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
		HaeinsaResult result = scanner.next();
		HaeinsaResult result2 = scanner.next();

		assertNull(result2);
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
		assertArrayEquals(result.getRow(), Bytes.toBytes("ymkim"));
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

		assertNull(result3);
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
		assertArrayEquals(result.getRow(), Bytes.toBytes("kjwoo"));
		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
		assertArrayEquals(result2.getRow(), Bytes.toBytes("ymkim"));
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
		tablePool.close();
	}

	@Test
	public void testConflictAndRecover() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableIfaceInternal testTable = (HaeinsaTableIfaceInternal) tablePool.getTable("test");
		HaeinsaTransaction tx = tm.begin();
		HaeinsaTransaction tx2 = tm.begin();

		HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432"));
		HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("kjwoo"));
		put2.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
		testTable.put(tx, put);
		testTable.put(tx, put2);

		testTable.put(tx2, put);
		HaeinsaTableTransaction tableState = tx2.createOrGetTableState(Bytes.toBytes("test"));
		HaeinsaRowTransaction rowState = tableState.createOrGetRowState(Bytes.toBytes("ymkim"));
		tx2.setPrewriteTimestamp(rowState.getCurrent().getCommitTimestamp() + 1);
		tx2.setCommitTimestamp(rowState.getCurrent().getCommitTimestamp() + 1);
		testTable.prewrite(rowState, Bytes.toBytes("ymkim"), true);

		try {
			tx.commit();
			assertTrue(false);
		} catch (Exception e) {
			assertTrue(e instanceof ConflictException);
		}

		tx = tm.begin();
		try {
			HaeinsaScan scan = new HaeinsaScan();
			HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
			HaeinsaResult result = scanner.next();
			HaeinsaResult result2 = scanner.next();

			assertNull(result2);
			assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
			assertArrayEquals(result.getRow(), Bytes.toBytes("ymkim"));
			scanner.close();
			tx.rollback();
			assertTrue(false);
		} catch (Exception e) {
			assertTrue(e instanceof ConflictException);
		}

		Thread.sleep(HaeinsaConstants.ROW_LOCK_TIMEOUT + 100);

		try {
			tx = tm.begin();
			HaeinsaScan scan = new HaeinsaScan();
			HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
			HaeinsaResult result = scanner.next();

			assertNull(result);
			scanner.close();

		} catch (Exception e) {
			assertTrue(false);
		}

		tx = tm.begin();
		HaeinsaScan scan = new HaeinsaScan();
		HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
		HaeinsaResult result = scanner.next();

		assertNull(result);
		scanner.close();

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
		HaeinsaResult result2 = scanner.next();
		HaeinsaResult result3 = scanner.next();

		assertNull(result3);
		assertArrayEquals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-9876-5432"));
		assertArrayEquals(result.getRow(), Bytes.toBytes("kjwoo"));
		assertArrayEquals(result2.getValue(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber")), Bytes.toBytes("010-1234-5678"));
		assertArrayEquals(result2.getRow(), Bytes.toBytes("ymkim"));
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
		tablePool.close();
	}

	/**
	 * Haeinsa 를 쓰지 않던 only-HBase 상태에서 lock 정보를 적으면서 Haeinsa 로 넘어가는 migration 을
	 * 위한 unit test 이다.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHBaseHaeinsaMigration() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		HTablePool hbasePool = new HTablePool(CONF, 128, PoolType.Reusable);
		HTableInterface hTestTable = hbasePool.getTable("test");

		/*
		 * HBase 의 row1 에 바로 put 을 한 후에 row1 에 Haeinsa Get 을 하고 다른 row2 에
		 * Haeinsa put 을 한 후에 transaction 을 commit 한다. row1 과 row2 가 모두 lock 정보가
		 * 적힌 Haeinsa 로 migration 되어야 한다. ( multiRowCommit() 을 사용하게 되므로 )
		 *
		 * 1. HBase put { row1, data, col1 }
		 * 2. Get { row1, data, col1 }
		 * 3. Put { row2, data, col2 }
		 * 4. Get { row1, data, col1 }
		 * 5. Get { row2, data, col2 }
		 */
		Put hPut = new Put(Bytes.toBytes("row1"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
		hTestTable.put(hPut);
		// no lock at { row1, row2 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row2")));

		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableIface testTable = tablePool.getTable("test");

		HaeinsaTransaction tx = tm.begin();
		HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
		HaeinsaResult result = testTable.get(tx, get);
		assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1")));
		HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("row2"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col2"), Bytes.toBytes("value2"));
		testTable.put(tx, put);
		tx.commit();

		tx = tm.begin();
		// check data on row1, row2
		get = new HaeinsaGet(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
		result = testTable.get(tx, get);
		assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1")));
		get = new HaeinsaGet(Bytes.toBytes("row2"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
		result = testTable.get(tx, get);
		assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col2")), Bytes.toBytes("value2")));
		tx.rollback();
		// still no have lock at { row1 }, have lock at { row2 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row2")));

		/*
		 * HBase 의 row3 에 바로 put 을 한 후에 row3 에 Haeinsa Put 을 하고 transaction 을
		 * commit 한다. row3 는 lock 정보가 적힌 Haeinsa 로 migration 되어야 한다.
		 *
		 * 1. HBase put { row3, data, col4 }
		 * 2. Put { row3, data, col3 }
		 */
		hPut = new Put(Bytes.toBytes("row3"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col3"), Bytes.toBytes("value3"));
		hTestTable.put(hPut);
		// no lock at { row3 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row3")));

		tx = tm.begin();
		put = new HaeinsaPut(Bytes.toBytes("row3"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col3"), Bytes.toBytes("value3-2.0"));
		testTable.put(tx, put);
		tx.commit();

		tx = tm.begin();
		get = new HaeinsaGet(Bytes.toBytes("row3"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col3"));
		result = testTable.get(tx, get);
		assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col3")), Bytes.toBytes("value3-2.0")));
		tx.rollback();
		// now have lock at { row3 }
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row3")));

		/*
		 * HBase 의 row4에 바로 put 을 한 후에 row4 에 Haeinsa Delete 를 하고 transaction 을
		 * commit 한다. row4 는 lock 정보가 적힌 Haeinsa 로 migration 하고 데이터는 비어 있어야 한다.
		 *
		 * 1. HBase put { row4, data, col4 }
		 * 2. Delete { row4, data, col4 }
		 */
		hPut = new Put(Bytes.toBytes("row4"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col4"), Bytes.toBytes("value4"));
		hTestTable.put(hPut);
		// no lock at { row4 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row4")));

		tx = tm.begin();
		HaeinsaDelete delete = new HaeinsaDelete(Bytes.toBytes("row4"));
		delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("col4"));
		testTable.delete(tx, delete);
		tx.commit();

		tx = tm.begin();
		result = testTable.get(tx, get);
		assertTrue(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col4")) == null);
		tx.rollback();
		// now have lock at { row4 }
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row4")));

		/*
		 * HBase 의 row5, row6, row7 에 바로 put 을 한 후에 row5 ~ row8 에 Haeinsa Scan 을
		 * 하고 row8 에 Haeinsa put 을 한 후에 Transaction 을 commit 한다.
		 *
		 * 1. HBase put { row5, data, col5 }
		 * 2. HBase put { row6, data, col6 }
		 * 3. HBase put { row7, data, col7 }
		 * 4. Scan { [ row5 ~ row8 ] }
		 * 5. Put { row8, data, col8 }
		 */
		// test HBase put -> Haeinsa Scan ( w\ multiRowCommit() ) Migration
		hPut = new Put(Bytes.toBytes("row5"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col5"), Bytes.toBytes("value5"));
		hTestTable.put(hPut);
		// no lock at { row5 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row5")));

		hPut = new Put(Bytes.toBytes("row6"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col6"), Bytes.toBytes("value6"));
		hTestTable.put(hPut);
		// no lock at { row6 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row6")));

		hPut = new Put(Bytes.toBytes("row7"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col7"), Bytes.toBytes("value7"));
		hTestTable.put(hPut);
		// no lock at { row7 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row7")));

		tx = tm.begin();
		HaeinsaScan scan = new HaeinsaScan();
		scan.setStartRow(Bytes.toBytes("row5"));
		scan.setStopRow(Bytes.toBytes("row8"));
		Iterator<HaeinsaResult> iter = testTable.getScanner(tx, scan).iterator();
		while (iter.hasNext()) {
			iter.next();
		}
		put = new HaeinsaPut(Bytes.toBytes("row8"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col8"), Bytes.toBytes("value8"));
		testTable.put(tx, put);
		tx.commit();

		// still no lock at {row5, row6, row7}, now have lock at {row8}
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row5")));
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row6")));
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row7")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row8")));

		tx = tm.begin();
		assertArrayEquals(testTable.get(tx,
				new HaeinsaGet(Bytes.toBytes("row5"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col5")),
				Bytes.toBytes("value5"));
		assertArrayEquals(testTable.get(tx,
				new HaeinsaGet(Bytes.toBytes("row6"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col6")),
				Bytes.toBytes("value6"));
		assertArrayEquals(testTable.get(tx,
				new HaeinsaGet(Bytes.toBytes("row7"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col7")),
				Bytes.toBytes("value7"));
		assertArrayEquals(testTable.get(tx,
				new HaeinsaGet(Bytes.toBytes("row8"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col8")),
				Bytes.toBytes("value8"));
		tx.rollback();

		/*
		 * HBase 의 row9 에 바로 여러 column 을 미리 put 한 후에 row9 에 intraScan 을 하고 row10
		 * 에 put 을 한 후에 Transaction 을 commit 한다. row10 과 row11 이 모두 lock 정보가 적힌
		 * Haeinsa 로 migration 되어야 한다. ( multiRowCommit() 을 사용하게 되므로 )
		 *
		 * 1. HBase put { row9, data, [col9-ver1, col9-ver2, col9-ver3] }
		 * 2. IntraScan { row9, data, col9 ~ col9-ver3 }
		 * 3. Put { row10, data, col10 }
		 */
		hPut = new Put(Bytes.toBytes("row9"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col9-ver1"), Bytes.toBytes("value9-ver1"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col9-ver2"), Bytes.toBytes("value9-ver2"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col9-ver3"), Bytes.toBytes("value9-ver3"));
		hTestTable.put(hPut);
		// no lock at { row9 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row9")));

		tx = tm.begin();
		HaeinsaIntraScan intraScan = new HaeinsaIntraScan(
				Bytes.toBytes("row9"),
				Bytes.toBytes("col9"), true,
				Bytes.toBytes("col9-ver3"), true);
		intraScan.setBatch(1);
		HaeinsaResultScanner resultScanner = testTable.getScanner(tx, intraScan);
		iter = resultScanner.iterator();
		assertArrayEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col9-ver1")), Bytes.toBytes("value9-ver1"));
		assertArrayEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col9-ver2")), Bytes.toBytes("value9-ver2"));
		assertArrayEquals(iter.next().getValue(Bytes.toBytes("data"), Bytes.toBytes("col9-ver3")), Bytes.toBytes("value9-ver3"));
		resultScanner.close();

		put = new HaeinsaPut(Bytes.toBytes("row10"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col10"), Bytes.toBytes("value10"));
		testTable.put(tx, put);
		tx.commit();
		// still have no lock at {row9}, now have lock at { row10 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row9")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row10")));

		/*
		 * 비어 있는 row11 에 intraScan 을 통해 read 를 시도한 후에 row10 에 HaeinsaPut 을 하고
		 * Transaction 을 commit 한다. row10 과 row11 은 모두 Haeinsa 로 migration 되고
		 * lock 을 보유해야 한다.
		 *
		 * 1. intraScan { row11, data, col11 ~ col11-ver3 } -> empty
		 * 2. Put { row10, data, col10 }
		 */
		byte[] row = Bytes.toBytes("row10");
		byte[] oldPutLock = getLock(hTestTable, row);
		// no lock at { row11 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row11")));

		tx = tm.begin();
		intraScan = new HaeinsaIntraScan(
				Bytes.toBytes("row11"),
				Bytes.toBytes("col11"), true,
				Bytes.toBytes("col11-ver3"), true);
		intraScan.addFamily(Bytes.toBytes("data"));
		intraScan.setBatch(1);
		resultScanner = testTable.getScanner(tx, intraScan);
		iter = resultScanner.iterator();
		resultScanner.close();

		put = new HaeinsaPut(Bytes.toBytes("row10"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col10"), Bytes.toBytes("value10"));
		testTable.put(tx, put);
		tx.commit();
		// lock at { row10 } changed
		assertTrue(checkLockChanged(hTestTable, row, oldPutLock));
		// still have no lock at { row11 }
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row11")));

		// release all resources
		hTestTable.close();
		testTable.close();
		tablePool.close();
		hbasePool.close();
	}

	/**
	 * 여러 개의 mutation 이 하나의 transaction 에 걸쳐서 들어왔을 때 정상적으로 동작하는 지 확인하기 위한 unit
	 * test 이다.
	 *
	 * @throws Exception
	 */
	@Test
	public void testMultipleMutations() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableIface testTable = tablePool.getTable("test");

		/*
		 * 일단 row-abc 라는 row 에 2개의 값과 row-d 라는 row 에 1개의 값을 put 한다.
		 *
		 * 1. put { row-abc, data, column-a }
		 * 2. put { row-abc, data, column-b }
		 * 3. put { row-d, meta, column-d }
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
		 * 4. put { row-abc, data, column-c }
		 * 5. put { row-e, meta, column-e }
		 * 6. deleteFamily { row-abc, data }
		 * 7. put { row-abc, data, col-after }
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
		 * 결과를 확인하기 위한 단계이다. 3, 5, 7 번의 put만 남아 있어야 한다.
		 *
		 * 3. put { row-d, meta, column-d }
		 * 5. put { row-e, meta, column-e }
		 * 7. put { row-abc, data, col-after } ( row-abc 의 column 이 유일해야 한다. )
		 */
		tx = tm.begin();
		HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row-d"));
		get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("column-d"));
		assertArrayEquals(testTable.get(tx, get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("column-d")),
				Bytes.toBytes("value-d"));

		get = new HaeinsaGet(Bytes.toBytes("row-e"));
		get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("column-e"));
		assertArrayEquals(testTable.get(tx, get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("column-e")),
				Bytes.toBytes("value-e"));

		get = new HaeinsaGet(Bytes.toBytes("row-abc"));
		HaeinsaResult result = testTable.get(tx, get);
		assertTrue(result.list().size() == 1);
		assertArrayEquals(testTable.get(tx, get).getValue(Bytes.toBytes("data"), Bytes.toBytes("col-after")),
				Bytes.toBytes("value-after"));

		tx.rollback();

		testTable.close();
		tablePool.close();
	}

	/**
	 * Haeinsa 에서 GetWithtoutTx / ScanWithtoutTx / IntraScanWithtoutTx 이 lock 을
	 * 바꾸지 않고 정상적인 동작을 하는지 확인하기 위한 unit test 이다.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHaeinsaTableWithoutTx() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		final HaeinsaTablePool tablePool = TestingUtility.createHaeinsaTablePool(CONF, threadPool);

		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableIface testTable = tablePool.getTable("test");
		HTablePool hbasePool = new HTablePool(CONF, 128, PoolType.Reusable);
		HTableInterface hTestTable = hbasePool.getTable("test");

		/*
		 * row-put-a 와 row-put-b 에 데이터를 쓴 후에 Transaction 을 commit 하고 새로운
		 * Transaction 을 만들어서 row-put-a 에는 GetWithoutTx, row-put-b 에는 Put 을 한 후에
		 * commit 한다. row-put-a 의 lock 은 바뀌지 않아야 하며, row-put-b 의 lock 은 바뀌어야 한다.
		 *
		 * 1. Put { row-put-a, data, col-put-a }
		 * 2. Put { row-put-b, data, col-put-b }
		 * 3. GetWithoutTx { row-put-a, data }
		 * 4. Put { row-put-b, data, col-put-b }
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
		assertFalse(checkLockChanged(hTestTable, row, oldLockGet));
		// lock at { row-put-b } changed
		row = Bytes.toBytes("row-put-b");
		assertTrue(checkLockChanged(hTestTable, row, oldLockPut));

		/*
		 * row-put-a 와 row-put-b 에 있는 데이터를 ScanWithoutTx 를 통해서 읽은 후에 row-put-c 에
		 * 새로운 값을 put 하고 Transaction 을 commit 한다. row-put-a 와 row-put-b 의 lock 은
		 * 바뀌지 않고, row-put-c 의 lock 은 바뀌어야 한다. 이 unit test 에서는 바로 위의 unit test
		 * 에서 put 한 데이터를 사용한다.
		 *
		 * 1. ScanWithtoutTx { row-put-a ~ row-put-c }
		 * 2. Put { row-put-c, data, col-put-c }
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
		assertFalse(checkLockChanged(hTestTable, row, oldLockScan1));
		// lock at { row-put-b } not changed
		row = Bytes.toBytes("row-put-b");
		assertFalse(checkLockChanged(hTestTable, row, oldLockScan2));
		// lock at { row-put-c } changed
		row = Bytes.toBytes("row-put-c");
		assertTrue(checkLockChanged(hTestTable, row, oldLockPut));

		/*
		 * row-put-d 의 column col-put-a, col-put-b, col-put-c 에 put 을 한 후에
		 * Transaction 을 commit 한다. 새로운 Transaction 을 시작한 후 row-put-d 에 있는 데이터를
		 * IntraScanWithoutTx 를 통해서 읽은 후에 row-put-e 에 새로운 값을 쓰고 Transaction 을
		 * commit 한다. row-put-d 의 lock 은 변하지 않고, row-put-e 의 lock 은 변해야 한다.
		 *
		 * 1. Put { row-put-d, data, [ col-put-a, col-put-b, col-put-c ] }
		 * 2. IntraScanWithoutTx { row-put-d, data, [ col-put-a ~ col-put-d ] }
		 * 3. Put { row-put-e, data, col-put-e }
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
		assertFalse(checkLockChanged(hTestTable, row, oldLockIntraScan));
		// lock at { row-put-e } changed
		row = Bytes.toBytes("row-put-e");
		assertTrue(checkLockChanged(hTestTable, row, oldLockPut));

		// release resources
		testTable.close();
		tablePool.close();
		hTestTable.close();
		hbasePool.close();
	}

	/**
	 * return true if TRowLock exist in specific ( table, row )
	 *
	 * @param table
	 * @param row
	 * @return
	 * @throws Exception
	 */
	private boolean checkLockExist(HTableInterface table, byte[] row) throws Exception {
		return getLock(table, row) != null;
	}

	private byte[] getLock(HTableInterface table, byte[] row) throws Exception {
		return table.get(new Get(row).addColumn(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER))
				.getValue(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER);
	}

	private boolean checkLockChanged(HTableInterface table, byte[] row, byte[] oldLock) throws Exception {
		return !Bytes.equals(getLock(table, row), oldLock);
	}
}
