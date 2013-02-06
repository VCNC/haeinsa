package kr.co.vcnc.haeinsa;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;
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
		
		TransactionManager tm = new TransactionManager(tablePool);
		HaeinsaTableInterface testTable = tablePool.getTable("test");
		Transaction tx = tm.begin();
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

		testTable.close();
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
		
		TransactionManager tm = new TransactionManager(tablePool);
		HaeinsaTableInterface testTable = tablePool.getTable("test");
		Transaction tx = tm.begin();
		Transaction tx2 = tm.begin();
		
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
		
		TransactionManager tm = new TransactionManager(tablePool);
		HaeinsaTable testTable = (HaeinsaTable) tablePool.getTable("test");
		Transaction tx = tm.begin();
		Transaction tx2 = tm.begin();
		
		HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-9876-5432")); 
		HaeinsaPut put2 = new HaeinsaPut(Bytes.toBytes("kjwoo"));
		put2.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
		testTable.put(tx, put);
		testTable.put(tx, put2);
		
		testTable.put(tx2, put);
		TableTransaction tableState = tx2.createOrGetTableState(Bytes.toBytes("test"));
		RowTransaction rowState = tableState.createOrGetRowState(Bytes.toBytes("ymkim"));
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
}
