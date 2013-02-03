package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

public class HaeinsaDeleteTrackerImpl implements HaeinsaDeleteTracker {
	private final NavigableMap<byte[], Long> families = Maps.newTreeMap(Bytes.BYTES_COMPARATOR); 
	

	@Override
	public void add(HaeinsaKeyValue kv, long sequenceID) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isDeleted(HaeinsaKeyValue kv, long sequenceID) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	
}
