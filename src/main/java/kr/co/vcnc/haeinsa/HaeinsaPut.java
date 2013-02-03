package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import kr.co.vcnc.haeinsa.thrift.generated.TCellKey;
import kr.co.vcnc.haeinsa.thrift.generated.TKeyValue;
import kr.co.vcnc.haeinsa.thrift.generated.TMutation;
import kr.co.vcnc.haeinsa.thrift.generated.TMutationType;
import kr.co.vcnc.haeinsa.thrift.generated.TPut;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

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
		this.familyMap = new TreeMap<byte[], NavigableSet<HaeinsaKeyValue>>(
				Bytes.BYTES_COMPARATOR);
		for (Map.Entry<byte[], NavigableSet<HaeinsaKeyValue>> entry : putToCopy.getFamilyMap()
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
		NavigableSet<HaeinsaKeyValue> set = getKeyValueSet(family);
		HaeinsaKeyValue kv = createPutKeyValue(family, qualifier, value);
		if (set.contains(kv)){
			set.remove(kv);
		}
		set.add(kv);
		familyMap.put(kv.getFamily(), set);
		return this;
	}

	/*
	 * Create a KeyValue with this objects row key and the Put identifier.
	 * 
	 * @return a KeyValue with this objects row key and the Put identifier.
	 */
	private HaeinsaKeyValue createPutKeyValue(byte[] family, byte[] qualifier,
			byte[] value) {
		return new HaeinsaKeyValue(this.row, family, qualifier, value, KeyValue.Type.Put);
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
	private NavigableSet<HaeinsaKeyValue> getKeyValueSet(byte[] family) {
		NavigableSet<HaeinsaKeyValue> list = familyMap.get(family);
		if (list == null) {
			list = Sets.newTreeSet(HaeinsaKeyValue.COMPARATOR);
		}
		return list;
	}
	
	@Override
	public void add(HaeinsaMutation newMutation) {
		Preconditions.checkState(!(newMutation instanceof HaeinsaPut));
		for (HaeinsaKeyValue newKV : Iterables.concat(newMutation.getFamilyMap().values())){
			add(newKV.getFamily(), newKV.getQualifier(), newKV.getValue());
		}
	}
		
	@Override
	public TMutation toTMutation() {
		TMutation newMutation = new TMutation();
		newMutation.setType(TMutationType.PUT);
		TPut put = new TPut();
		for (HaeinsaKeyValue kv : Iterables.concat(familyMap.values())){
			TKeyValue newKV = new TKeyValue();
			newKV.setKey(new TCellKey().setFamily(kv.getFamily()).setQualifier(kv.getQualifier()));
			newKV.setValue(kv.getValue());
			put.addToValues(newKV);
		}
		newMutation.setPut(put);
		return newMutation;
	}
}
