package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import kr.co.vcnc.haeinsa.thrift.generated.TMutation;

import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class HaeinsaMutation {
	protected byte[] row = null;
	protected Map<byte[], NavigableSet<HaeinsaKeyValue>> familyMap = new TreeMap<byte[], NavigableSet<HaeinsaKeyValue>>(
			Bytes.BYTES_COMPARATOR);


	/**
	 * Method for retrieving the put's familyMap
	 * 
	 * @return familyMap
	 */
	public Map<byte[], NavigableSet<HaeinsaKeyValue>> getFamilyMap() {
		return this.familyMap;
	}

	/**
	 * Method for setting the put's familyMap
	 */
	public void setFamilyMap(Map<byte[], NavigableSet<HaeinsaKeyValue>> map) {
		this.familyMap = map;
	}
	
	public Set<byte[]> getFamilies(){
		return familyMap.keySet();
	}

	/**
	 * Method to check if the familyMap is empty
	 * 
	 * @return true if empty, false otherwise
	 */
	public boolean isEmpty() {
		return familyMap.isEmpty();
	}

	/**
	 * Method for retrieving the delete's row
	 * 
	 * @return row
	 */
	public byte[] getRow() {
		return this.row;
	}

	public int compareTo(final Row d) {
		return Bytes.compareTo(this.getRow(), d.getRow());
	}
	
	public abstract void add(HaeinsaMutation newMutation);
	
	public abstract TMutation toTMutation();
	
	public abstract HaeinsaKeyValueScanner getScanner(byte[] family);
}
