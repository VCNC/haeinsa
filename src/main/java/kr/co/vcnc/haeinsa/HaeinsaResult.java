package kr.co.vcnc.haeinsa;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Modified POJO container of {@link Result} class in HBase.
 * Link {@link Result}, can contain multiple {@link HaeinsaKeyValue}.
 * All HaeinsaKeyValue in HaeinsaResult are from same row.
 * 
 * @author Myungbo Kim
 *
 */
public class HaeinsaResult {
	private final List<HaeinsaKeyValue> sortedKVs;
	private byte[] row = null;
	
	
	/**
	 * Construct HaeinsaResult from Result
	 * @param result HBase's result
	 */
	public HaeinsaResult(Result result){
		if (result.isEmpty()){
			List<HaeinsaKeyValue> emptyList = Collections.emptyList();
			this.sortedKVs = emptyList;
		}else{
			List<HaeinsaKeyValue> transformed = Lists.transform(result.list(), new Function<KeyValue, HaeinsaKeyValue>() {
				
				@Override
				public HaeinsaKeyValue apply(KeyValue kv){
					return new HaeinsaKeyValue(kv);
				}
			});
			this.sortedKVs = transformed;
		}
	}

	/**
	 * Construct HaeinsaResultImpl from sorted list of HaeinsaKeyValue
	 * @param sortedKVs - assume all the HaeinsaKeyValue in sortedKVs have same row and sorted in ascending order.
	 */
	public HaeinsaResult(List<HaeinsaKeyValue> sortedKVs) {
		this.sortedKVs = sortedKVs;
		if (sortedKVs.size() > 0) {
			row = sortedKVs.get(0).getRow();
		}
	}

	public byte[] getRow() {
		return row;
	}

	public List<HaeinsaKeyValue> list() {
		return sortedKVs;
	}

	public byte[] getValue(byte[] family, byte[] qualifier) {
	    // pos === ( -(insertion point) - 1)
		int pos = Collections.binarySearch(sortedKVs, new HaeinsaKeyValue(row,
				family, qualifier, null, KeyValue.Type.Maximum),
				HaeinsaKeyValue.COMPARATOR);
		// never will exact match
	    if (pos < 0) {
	    	pos = (pos+1) * -1;
	    	// pos is now insertion point
	    }
	    if (pos == sortedKVs.size()) {
	    	return null;
	    }
	    HaeinsaKeyValue kv = sortedKVs.get(pos);
	    if (kv.matchingColumn(family, qualifier)){
	    	return kv.getValue();
	    }
		return null;
	}

	public boolean containsColumn(byte[] family, byte[] qualifier) {
		return getValue(family, qualifier) != null;
	}

	public boolean isEmpty() {
		return sortedKVs.size() == 0;
	}
}
