package kr.co.vcnc.haeinsa;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HaeinsaTest {
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
		
		//	Table	->	ColumnFamily 
		//	{ test } -> { !lock!, data, meta }
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
		
		//	Table	->	ColumnFamily 
		//	{ log } -> { !lock!, raw }
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
	public void testTransaction() throws Exception{
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		HaeinsaTablePool tablePool = new HaeinsaTablePool(CONF, 128, new HTableInterfaceFactory() {
			
			@Override
			public void releaseHTableInterface(HTableInterface table)
					throws IOException {
				table.close();
			}
			
			@Override
			public HTableInterface createHTableInterface(Configuration config,
					byte[] tableName) {
				try {
					return new HTable(tableName, HConnectionManager.getConnection(config), threadPool);
				} catch (ZooKeeperConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});
		
		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableInterface testTable = tablePool.getTable("test");
		
		//	Test 2 puts tx
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
		
		//	test Table-cross transaction & multi-Column transaction
		tx = tm.begin();
		HaeinsaTableInterface logTable = tablePool.getTable("log");
		put = new HaeinsaPut(Bytes.toBytes("previousTime"));
		put.add(Bytes.toBytes("raw"), Bytes.toBytes("time-0"), Bytes.toBytes("log-value-1"));
		logTable.put(tx, put);
		put = new HaeinsaPut(Bytes.toBytes("row-0"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("time-0"), Bytes.toBytes("data-value-1"));
		put.add(Bytes.toBytes("meta"), Bytes.toBytes("time-0"), Bytes.toBytes("meta-value-1"));
		testTable.put(tx, put);
		tx.commit();

		//		check tx result 
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
		
		
		//	clear test - table
		tx = tm.begin();
		scan = new HaeinsaScan();
		scanner = testTable.getScanner(tx, scan);
		Iterator<HaeinsaResult> iter = scanner.iterator();
		while(iter.hasNext()){
			result = iter.next();
			for(HaeinsaKeyValue kv : result.list()){
				kv.getRow();
				//	delete specific kv - delete only if it's not lock family
				HaeinsaDelete delete = new HaeinsaDelete(kv.getRow());
				//	should not return lock by scanner
				assertFalse(Bytes.equals(kv.getFamily(),HaeinsaConstants.LOCK_FAMILY));
				delete.deleteColumns(kv.getFamily(), kv.getQualifier());
				testTable.delete(tx, delete);
				
			}
		}
		tx.commit();
		scanner.close();
		
		//	clear log - table
		tx = tm.begin();
		scan = new HaeinsaScan();
		scanner = logTable.getScanner(tx, scan);
		iter = scanner.iterator();
		while(iter.hasNext()){
			result = iter.next();
			for(HaeinsaKeyValue kv : result.list()){
				kv.getRow();
				//	delete specific kv - delete only if it's not lock family
				HaeinsaDelete delete = new HaeinsaDelete(kv.getRow());
				//	should not return lock by scanner
				assertFalse(Bytes.equals(kv.getFamily(),HaeinsaConstants.LOCK_FAMILY));
				delete.deleteColumns(kv.getFamily(), kv.getQualifier());
				logTable.delete(tx, delete);
			}
		}
		tx.commit();
		scanner.close();
		
		//	check whether table is clear - testTable
		tx = tm.begin();
		scan = new HaeinsaScan();
		scanner = testTable.getScanner(tx, scan);
		iter = scanner.iterator();
		assertFalse(iter.hasNext());
		tx.rollback();
		scanner.close();
		//	check whether table is clear - logTable
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
		HaeinsaTablePool tablePool = new HaeinsaTablePool(CONF, 128, new HTableInterfaceFactory() {
			
			@Override
			public void releaseHTableInterface(HTableInterface table)
					throws IOException {
				table.close();
			}
			
			@Override
			public HTableInterface createHTableInterface(Configuration config,
					byte[] tableName) {
				try {
					return new HTable(tableName, HConnectionManager.getConnection(config), threadPool);
				} catch (ZooKeeperConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});
		
		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableInterface testTable = tablePool.getTable("test");
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
		try{
			tx.commit();
			assertTrue(false);
		}catch(Exception e){
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
		HaeinsaTablePool tablePool = new HaeinsaTablePool(CONF, 128, new HTableInterfaceFactory() {
			
			@Override
			public void releaseHTableInterface(HTableInterface table)
					throws IOException {
				table.close();
			}
			
			@Override
			public HTableInterface createHTableInterface(Configuration config,
					byte[] tableName) {
				try {
					return new HTable(tableName, HConnectionManager.getConnection(config), threadPool);
				} catch (ZooKeeperConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});
		
		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTable testTable = (HaeinsaTable) tablePool.getTable("test");
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
		
		try{
			tx.commit();
			assertTrue(false);
		}catch(Exception e){
			assertTrue(e instanceof ConflictException);
		}
		
		tx = tm.begin();
		try{
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
		}catch(Exception e){
			assertTrue(e instanceof ConflictException);
		}
		
		Thread.sleep(HaeinsaConstants.ROW_LOCK_TIMEOUT + 100);
		
		try{
			tx = tm.begin();
			HaeinsaScan scan = new HaeinsaScan();
			HaeinsaResultScanner scanner = testTable.getScanner(tx, scan);
			HaeinsaResult result = scanner.next();
			
			assertNull(result);
			scanner.close();

		}catch(Exception e) {
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
	

	@Test
	public void testHBaseHaeinsaMigration() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		HaeinsaTablePool tablePool = new HaeinsaTablePool(CONF, 128, new HTableInterfaceFactory() {
			
			@Override
			public void releaseHTableInterface(HTableInterface table)
					throws IOException {
				table.close();
			}
			
			@Override
			public HTableInterface createHTableInterface(Configuration config,
					byte[] tableName) {
				try {
					return new HTable(tableName, HConnectionManager.getConnection(config), threadPool);
				} catch (ZooKeeperConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});
		
		
		//	test HBase put -> Haeinsa Get ( w\ multiRowCommit() ) Migration
		HTablePool hbasePool = new HTablePool(CONF,128,PoolType.Reusable);
		HTableInterface hTestTable = hbasePool.getTable("test");
		Put hPut = new Put(Bytes.toBytes("row1"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
		hTestTable.put(hPut);
		//	no lock
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row1")));
		
		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableInterface testTable = tablePool.getTable("test");
		
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
		get = new HaeinsaGet(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col1"));
		result = testTable.get(tx, get);
		assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col1")), Bytes.toBytes("value1")));
		get = new HaeinsaGet(Bytes.toBytes("row2"));
		get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("col2"));
		result = testTable.get(tx, get);
		assertTrue(Bytes.equals(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("col2")), Bytes.toBytes("value2")));
		tx.rollback();
		//	now have lock
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row1")));

		
		//	test HBase put -> Haeinsa Put ( w\ multiRowCommit() ) Migration
		hPut = new Put(Bytes.toBytes("row3"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col3"), Bytes.toBytes("value3"));
		hTestTable.put(hPut);
		//	no lock
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
		//	now have lock
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row3")));
		
		
		//	test HBase put -> Haeinsa Delete ( w\ multiRowCommit() ) Migration
		hPut = new Put(Bytes.toBytes("row4"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col4"), Bytes.toBytes("value4"));
		hTestTable.put(hPut);
		//	no lock
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
		//	now have lock
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row4")));
		

		//	test HBase put -> Haeinsa Scan ( w\ multiRowCommit() ) Migration
		hPut = new Put(Bytes.toBytes("row5"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col5"), Bytes.toBytes("value5"));
		hTestTable.put(hPut);
		//	no lock
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row5")));
		
		hPut = new Put(Bytes.toBytes("row6"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col6"), Bytes.toBytes("value6"));
		hTestTable.put(hPut);
		//	no lock
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row6")));
		
		hPut = new Put(Bytes.toBytes("row7"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col7"), Bytes.toBytes("value7"));
		hTestTable.put(hPut);
		//	no lock
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row7")));
		
		tx = tm.begin();
		HaeinsaScan scan = new HaeinsaScan();
		scan.setStartRow(Bytes.toBytes("row5"));
		scan.setStopRow(Bytes.toBytes("row8"));
		Iterator<HaeinsaResult> iter = testTable.getScanner(tx, scan).iterator();
		while(iter.hasNext()){
			iter.next();
		}		
		put = new HaeinsaPut(Bytes.toBytes("row8"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col8"), Bytes.toBytes("value8"));
		testTable.put(tx, put);
		tx.commit();
		
		//	now have lock
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row5")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row6")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row7")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row8")));
		
		tx = tm.begin();
		assertArrayEquals(testTable.get(tx, new HaeinsaGet(Bytes.toBytes("row5"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col5")), Bytes.toBytes("value5"));
		assertArrayEquals(testTable.get(tx, new HaeinsaGet(Bytes.toBytes("row6"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col6")), Bytes.toBytes("value6"));
		assertArrayEquals(testTable.get(tx, new HaeinsaGet(Bytes.toBytes("row7"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col7")), Bytes.toBytes("value7"));
		assertArrayEquals(testTable.get(tx, new HaeinsaGet(Bytes.toBytes("row8"))).getValue(Bytes.toBytes("data"), Bytes.toBytes("col8")), Bytes.toBytes("value8"));
		tx.rollback();
		
		//	test HBase put -> Haeinsa intraScan ( w\ multiRowCommit() ) Migration
		hPut = new Put(Bytes.toBytes("row9"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col9-ver1"), Bytes.toBytes("value9-ver1"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col9-ver2"), Bytes.toBytes("value9-ver2"));
		hPut.add(Bytes.toBytes("data"), Bytes.toBytes("col9-ver3"), Bytes.toBytes("value9-ver3"));
		hTestTable.put(hPut);
		//	no lock
		assertFalse(checkLockExist(hTestTable, Bytes.toBytes("row9")));
		
		tx = tm.begin();
		HaeinsaIntraScan intraScan = new HaeinsaIntraScan(
				Bytes.toBytes("row9"), 
				Bytes.toBytes("col9"), true, 
				Bytes.toBytes("col9-ver3"), true);
		intraScan.setBatch(1);
		HaeinsaResultScanner resultScanner = testTable.getScanner(tx, intraScan);
		iter = resultScanner.iterator();
		assertArrayEquals(iter.next().getValue(Bytes.toBytes("data"),Bytes.toBytes("col9-ver1")), Bytes.toBytes("value9-ver1"));
		assertArrayEquals(iter.next().getValue(Bytes.toBytes("data"),Bytes.toBytes("col9-ver2")), Bytes.toBytes("value9-ver2"));
		assertArrayEquals(iter.next().getValue(Bytes.toBytes("data"),Bytes.toBytes("col9-ver3")), Bytes.toBytes("value9-ver3"));
		resultScanner.close();

		put = new HaeinsaPut(Bytes.toBytes("row10"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col10"), Bytes.toBytes("value10"));
		testTable.put(tx, put);
		tx.commit();
		//	now have lock
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row9")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row10")));
		
		
		//	access to Empty row
		tx = tm.begin();
		intraScan = new HaeinsaIntraScan(
				Bytes.toBytes("row11"), 
				Bytes.toBytes("col11"), true, 
				Bytes.toBytes("col11-ver3"), true);
		intraScan.setBatch(1);
		resultScanner = testTable.getScanner(tx, intraScan);
		iter = resultScanner.iterator();
		resultScanner.close();

		put = new HaeinsaPut(Bytes.toBytes("row10"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("col10"), Bytes.toBytes("value10"));
		testTable.put(tx, put);
		tx.commit();
		//	now have lock
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row11")));
		assertTrue(checkLockExist(hTestTable, Bytes.toBytes("row10")));
		
		
		hTestTable.close();
		testTable.close();
		tablePool.close();
		hbasePool.close();
	}

	@Test
	public void testMultipleMutations() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		HaeinsaTablePool tablePool = new HaeinsaTablePool(CONF, 128, new HTableInterfaceFactory() {
			
			@Override
			public void releaseHTableInterface(HTableInterface table)
					throws IOException {
				table.close();
			}
			
			@Override
			public HTableInterface createHTableInterface(Configuration config,
					byte[] tableName) {
				try {
					return new HTable(tableName, HConnectionManager.getConnection(config), threadPool);
				} catch (ZooKeeperConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});		
		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableInterface testTable = tablePool.getTable("test");
		
		//	put, put, put
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
		
		//	put, deleteFamily, put
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
		
		
		//	check result
		tx = tm.begin();
		HaeinsaGet get = new HaeinsaGet(Bytes.toBytes("row-abc"));
		HaeinsaResult result = testTable.get(tx, get);
		//		there should be only 1 HaeinsaKeyValue on row-abc
		assertTrue(result.list().size() == 1);
		get = new HaeinsaGet(Bytes.toBytes("row-d"));
		get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("column-d"));
		assertArrayEquals(testTable.get(tx,get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("column-d")), 
				Bytes.toBytes("value-d"));
		get = new HaeinsaGet(Bytes.toBytes("row-e"));
		get.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("column-e"));
		assertArrayEquals(testTable.get(tx,get).getValue(Bytes.toBytes("meta"), Bytes.toBytes("column-e")), 
				Bytes.toBytes("value-e"));
		tx.commit();
		
		
		testTable.close();
		tablePool.close();
	}
	

	@Test
	public void testHaeinsaTableWithoutTx() throws Exception {
		final ExecutorService threadPool = Executors.newCachedThreadPool();
		HaeinsaTablePool tablePool = new HaeinsaTablePool(CONF, 128, new HTableInterfaceFactory() {
			
			@Override
			public void releaseHTableInterface(HTableInterface table)
					throws IOException {
				table.close();
			}
			
			@Override
			public HTableInterface createHTableInterface(Configuration config,
					byte[] tableName) {
				try {
					return new HTable(tableName, HConnectionManager.getConnection(config), threadPool);
				} catch (ZooKeeperConnectionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});		
		HaeinsaTransactionManager tm = new HaeinsaTransactionManager(tablePool);
		HaeinsaTableInterface testTable = tablePool.getTable("test");
		
		//	getWithoutTx
		HaeinsaTransaction tx = tm.begin();
		
		
		
		tx.commit();
		
		//	getScannerWithoutTx ( HaeinsaScan )
		tx = tm.begin();
		
		
		
		tx.commit();
		
		
		//	getScannerWithoutTx ( HaeinsaIntrascan )
		tx = tm.begin();
		
		
		
		tx.commit();
		
		
		testTable.close();
		tablePool.close();
	}
	
	/**
	 * return true if TRowLock exist in specific ( table, row ) 
	 * @param table
	 * @param row
	 * @return
	 * @throws Exception
	 */
	public boolean checkLockExist(HTableInterface table, byte[] row) throws Exception{
		return getLock(table, row) != null; 
	}
	
	public byte[] getLock(HTableInterface table, byte[] row) throws Exception{
		return table.get(new Get(row).addColumn(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER))
				.getValue(HaeinsaConstants.LOCK_FAMILY, HaeinsaConstants.LOCK_QUALIFIER);
	}
	
	public boolean checkLockChanged(HTableInterface table, byte[] row, byte[] oldLock) throws Exception{
		return !Bytes.equals(getLock(table, row), oldLock);
	}
}