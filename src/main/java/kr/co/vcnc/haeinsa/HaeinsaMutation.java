package kr.co.vcnc.haeinsa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class HaeinsaMutation {
	protected byte[] row = null;
	protected Map<byte[], List<KeyValue>> familyMap = new TreeMap<byte[], List<KeyValue>>(
			Bytes.BYTES_COMPARATOR);

	/**
	 * Compile the column family (i.e. schema) information into a Map. Useful
	 * for parsing and aggregation by debugging, logging, and administration
	 * tools.
	 * 
	 * @return Map
	 */
	public Map<String, Object> getFingerprint() {
		Map<String, Object> map = new HashMap<String, Object>();
		List<String> families = new ArrayList<String>();
		// ideally, we would also include table information, but that
		// information
		// is not stored in each Operation instance.
		map.put("families", families);
		for (Map.Entry<byte[], List<KeyValue>> entry : this.familyMap
				.entrySet()) {
			families.add(Bytes.toStringBinary(entry.getKey()));
		}
		return map;
	}

	/**
	 * Compile the details beyond the scope of getFingerprint (row, columns,
	 * timestamps, etc.) into a Map along with the fingerprinted information.
	 * Useful for debugging, logging, and administration tools.
	 * 
	 * @param maxCols
	 *            a limit on the number of columns output prior to truncation
	 * @return Map
	 */
	public Map<String, Object> toMap(int maxCols) {
		// we start with the fingerprint map and build on top of it.
		Map<String, Object> map = getFingerprint();
		// replace the fingerprint's simple list of families with a
		// map from column families to lists of qualifiers and kv details
		Map<String, List<Map<String, Object>>> columns = new HashMap<String, List<Map<String, Object>>>();
		map.put("families", columns);
		map.put("row", Bytes.toStringBinary(this.row));
		int colCount = 0;
		// iterate through all column families affected
		for (Map.Entry<byte[], List<KeyValue>> entry : this.familyMap
				.entrySet()) {
			// map from this family to details for each kv affected within the
			// family
			List<Map<String, Object>> qualifierDetails = new ArrayList<Map<String, Object>>();
			columns.put(Bytes.toStringBinary(entry.getKey()), qualifierDetails);
			colCount += entry.getValue().size();
			if (maxCols <= 0) {
				continue;
			}
			// add details for each kv
			for (KeyValue kv : entry.getValue()) {
				if (--maxCols <= 0) {
					continue;
				}
				Map<String, Object> kvMap = kv.toStringMap();
				// row and family information are already available in the
				// bigger map
				kvMap.remove("row");
				kvMap.remove("family");
				qualifierDetails.add(kvMap);
			}
		}
		map.put("totalColumns", colCount);
		return map;
	}

	/**
	 * Method for retrieving the put's familyMap
	 * 
	 * @return familyMap
	 */
	public Map<byte[], List<KeyValue>> getFamilyMap() {
		return this.familyMap;
	}

	/**
	 * Method for setting the put's familyMap
	 */
	public void setFamilyMap(Map<byte[], List<KeyValue>> map) {
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
	
	public abstract HaeinsaKeyValueScanner getScanner(byte[] family);
}
