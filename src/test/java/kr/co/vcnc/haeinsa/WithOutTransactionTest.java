package kr.co.vcnc.haeinsa;

import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WithOutTransactionTest extends HaeinsaTestBase {

    @Test
    public void testGet() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface testTable = context().getHaeinsaTableIface("test");

        byte[] row = Bytes.toBytes("row");
        byte[] family = Bytes.toBytes("data");
        byte[] col1 = Bytes.toBytes("col1");
        byte[] col2 = Bytes.toBytes("col2");

        // Insert data wih transaction
        {
            HaeinsaTransaction tx = tm.begin();
            HaeinsaPut put = new HaeinsaPut(row);
            put.add(family, col1, Bytes.toBytes("value1"));
            testTable.put(tx, put);
            put = new HaeinsaPut(row);
            put.add(family, col2, Bytes.toBytes("value2"));
            testTable.put(tx, put);
            tx.commit();
        }
        // Get with transaction
        {

            HaeinsaTransaction tx = tm.begin();
            HaeinsaGet get = new HaeinsaGet(row);
            get.addFamily(family);
            HaeinsaResult result = testTable.get(tx, get);
            tx.commit();

            byte[] value1 = result.getValue(family, col1);
            byte[] value2 = result.getValue(family, col2);

            Assert.assertEquals(result.containsColumn(family, col1), true);
            Assert.assertEquals(result.containsColumn(family, col2), true);
            Assert.assertEquals(value1, Bytes.toBytes("value1"));
            Assert.assertEquals(value2, Bytes.toBytes("value2"));
        }
        // Get without transaction
        {
            HaeinsaGet get = new HaeinsaGet(row);
            get.addFamily(family);
            HaeinsaResult result = testTable.get(null, get);

            byte[] value1 = result.getValue(family, col1);
            byte[] value2 = result.getValue(family, col2);

            Assert.assertEquals(result.containsColumn(family, col1), true);
            Assert.assertEquals(result.containsColumn(family, col2), true);
            Assert.assertEquals(value1, Bytes.toBytes("value1"));
            Assert.assertEquals(value2, Bytes.toBytes("value2"));
        }
    }
}
