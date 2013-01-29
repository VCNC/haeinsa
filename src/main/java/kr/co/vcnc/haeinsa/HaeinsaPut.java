package kr.co.vcnc.haeinsa;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class HaeinsaPut extends HaeinsaMutation {
	public HaeinsaPut(byte[] row) {
		this.row = row;
	}

	/**
	 * Copy constructor. Creates a Put operation cloned from the specified Put.
	 * 
	 * @param putToCopy
	 *            put to copy
	 */
	public HaeinsaPut(HaeinsaPut putToCopy) {
		this(putToCopy.getRow());
		this.familyMap = new TreeMap<byte[], List<KeyValue>>(
				Bytes.BYTES_COMPARATOR);
		for (Map.Entry<byte[], List<KeyValue>> entry : putToCopy.getFamilyMap()
				.entrySet()) {
			this.familyMap.put(entry.getKey(), entry.getValue());
		}
	}
	
	/**
	   * Add the specified column and value to this Put operation.
	   * @param family family name
	   * @param qualifier column qualifier
	   * @param value column value
	   * @return this
	   */
	  public HaeinsaPut add(byte [] family, byte [] qualifier, byte [] value) {
	    return add(family, qualifier, HConstants.LATEST_TIMESTAMP, value);
	  }

	/**
	 * Add the specified column and value, with the specified timestamp as its
	 * version to this Put operation.
	 * 
	 * @param family
	 *            family name
	 * @param qualifier
	 *            column qualifier
	 * @param ts
	 *            version timestamp
	 * @param value
	 *            column value
	 * @return this
	 */
	private HaeinsaPut add(byte[] family, byte[] qualifier, long ts, byte[] value) {
		List<KeyValue> list = getKeyValueList(family);
		KeyValue kv = createPutKeyValue(family, qualifier, ts, value);
		list.add(kv);
		familyMap.put(kv.getFamily(), list);
		return this;
	}

	/*
	 * Create a KeyValue with this objects row key and the Put identifier.
	 * 
	 * @return a KeyValue with this objects row key and the Put identifier.
	 */
	private KeyValue createPutKeyValue(byte[] family, byte[] qualifier,
			long ts, byte[] value) {
		return new KeyValue(this.row, family, qualifier, ts, KeyValue.Type.Put,
				value);
	}

	/**
	 * Creates an empty list if one doesnt exist for the given column family or
	 * else it returns the associated list of KeyValue objects.
	 * 
	 * @param family
	 *            column family
	 * @return a list of KeyValue objects, returns an empty list if one doesnt
	 *         exist.
	 */
	private List<KeyValue> getKeyValueList(byte[] family) {
		List<KeyValue> list = familyMap.get(family);
		if (list == null) {
			list = new ArrayList<KeyValue>(0);
		}
		return list;
	}
	
	@Override
	public HaeinsaKeyValueScanner getScanner(byte[] family) {
		// TODO Auto-generated method stub
		return null;
	}
}
