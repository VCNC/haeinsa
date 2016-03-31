package kr.co.vcnc.haeinsa;

import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HaeinsaMutationTest {
    @Test
    public void testRemove() throws Exception {
        HaeinsaDelete delete = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        delete.deleteFamily(Bytes.toBytes("data2"));
        delete.deleteColumns(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"));
        HaeinsaPut put = new HaeinsaPut(Bytes.toBytes("ymkim"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("phoneNumber"), Bytes.toBytes("010-1234-5678"));
        put.add(Bytes.toBytes("data"), Bytes.toBytes("address"), Bytes.toBytes("Seoul"));
        put.add(Bytes.toBytes("data2"), Bytes.toBytes("name"), Bytes.toBytes("ymkim"));

        delete.remove(put);

        HaeinsaDelete expectedDelete = new HaeinsaDelete(Bytes.toBytes("ymkim"));
        expectedDelete.deleteFamily(Bytes.toBytes("data2"));

        Assert.assertEquals(delete.toTMutation(), expectedDelete.toTMutation());
    }
}
