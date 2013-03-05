package kr.co.vcnc.haeinsa;

import java.util.List;
import org.apache.hadoop.hbase.client.Result;

/**
 * Modified POJO container of {@link Result} class in HBase.
 * Link {@link Result}, can contain multiple {@link HaeinsaKeyValue}.
 * All HaeinsaKeyValue in HaeinsaResult are from same row.
 * 
 * @author Myungbo Kim
 *
 */
public interface HaeinsaResult {
	byte[] getRow();
	List<HaeinsaKeyValue> list();
	byte[] getValue(byte[] family, byte[] qualifier);
	boolean containsColumn(byte[] family, byte[] qualifier);
	boolean isEmpty();
}
