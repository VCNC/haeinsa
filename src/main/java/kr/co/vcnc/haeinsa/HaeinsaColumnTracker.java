package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Sets;

public class HaeinsaColumnTracker {
	private final Map<byte[], NavigableSet<byte[]>> familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(
			Bytes.BYTES_COMPARATOR);
	
	
	public HaeinsaColumnTracker(Map<byte[], NavigableSet<byte[]>> familyMap){
		for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()){
			if (entry.getValue() != null){
				NavigableSet<byte[]> set = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
				set.addAll(entry.getValue());
				this.familyMap.put(entry.getKey(), set);
			}else{
				this.familyMap.put(entry.getKey(), null);
			}
		}
	}
	
	public boolean isMatched(HaeinsaKeyValue kv){
		if (familyMap.isEmpty()){
			return true;
		}
		NavigableSet<byte[]> set = familyMap.get(kv.getFamily());
		if (set == null){
			return true;
		}
		if (set.contains(kv.getQualifier())){
			return true;
		}
		
		return false;
	}
}
