package kr.co.vcnc.haeinsa;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;

public class HaeinsaDeleteTracker {
	private final NavigableMap<byte[], Long> families = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
	private final NavigableMap<byte[], NavigableMap<byte[], Long>> cells = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

	public void add(HaeinsaKeyValue kv, long sequenceID) {
		switch (kv.getType()) {
		case DeleteFamily:{
			Long previous = families.get(kv.getFamily());
			if (previous == null || previous.compareTo(sequenceID) > 0){
				families.put(kv.getFamily(), sequenceID);
			}
			break;
		}
		
		case DeleteColumn: {
			NavigableMap<byte[], Long> cellMap = cells.get(kv.getFamily());
			if (cellMap == null){
				cellMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
				cells.put(kv.getFamily(), cellMap);
			}
			Long previous = families.get(kv.getQualifier());
			if (previous == null || previous.compareTo(sequenceID) > 0){
				cellMap.put(kv.getQualifier(), sequenceID);
			}
			break;
		}
		
		default:
			break;
		} 
	}

	public boolean isDeleted(HaeinsaKeyValue kv, long sequenceID) {
		// check family
		Long deletedSequenceID = families.get(kv.getFamily());
		if (deletedSequenceID != null && deletedSequenceID.compareTo(sequenceID) < 0){
			return true;
		}
		
		// check cell
		NavigableMap<byte[], Long> cellMap = cells.get(kv.getFamily());
		if (cellMap != null){
			deletedSequenceID = cellMap.get(kv.getQualifier());
			if (deletedSequenceID != null && deletedSequenceID.compareTo(sequenceID) < 0){
				return true;
			}
		}
		return false;
	}

	public void reset() {
		families.clear();
		cells.clear();
	}
	
}
