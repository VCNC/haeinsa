package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import kr.co.vcnc.haeinsa.thrift.generated.TCellKey;
import kr.co.vcnc.haeinsa.thrift.generated.TKeyValue;
import kr.co.vcnc.haeinsa.thrift.generated.TMutation;
import kr.co.vcnc.haeinsa.thrift.generated.TMutationType;
import kr.co.vcnc.haeinsa.thrift.generated.TPut;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Implementation of {@link HaeinsaMuation} which only contains HaeinsaKeyValue with {@link Type#Put} identifier.
 * <p>HaeinsaPut only contains data of single row.
 * @author Myungbo Kim
 *
 */
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
	public HaeinsaPut add(byte[] family, byte[] qualifier, byte[] value) {
		NavigableSet<HaeinsaKeyValue> set = getKeyValueSet(family);
		HaeinsaKeyValue kv = createPutKeyValue(family, qualifier, value);
		/*
		if (set.contains(kv)) {
			set.remove(kv);
		}
		set.add(kv);
		*/
		if(!set.contains(kv)){
			set.add(kv);
		}
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
	 * Creates an empty set if one doesn't exist for the given column family or
	 * else it returns the associated set of KeyValue objects.
	 * 
	 * @param family
	 *            column family
	 * @return a set of KeyValue objects, returns an empty set if one doesn't
	 *         exist.
	 */
	private NavigableSet<HaeinsaKeyValue> getKeyValueSet(byte[] family) {
		NavigableSet<HaeinsaKeyValue> set = familyMap.get(family);
		if (set == null) {
			set = Sets.newTreeSet(HaeinsaKeyValue.COMPARATOR);
		}
		return set;
	}
	
	/**
	 * Merge all familyMap to this instance.
	 */
	@Override
	public void add(HaeinsaMutation newMutation) {
		Preconditions.checkState(newMutation instanceof HaeinsaPut);
		for (HaeinsaKeyValue newKV : Iterables.concat(newMutation.getFamilyMap().values())){
			add(newKV.getFamily(), newKV.getQualifier(), newKV.getValue());
		}
	}
		
	@Override
	public TMutation toTMutation() {
		TMutation newTMutation = new TMutation();
		newTMutation.setType(TMutationType.PUT);
		TPut newTPut = new TPut();
		for (HaeinsaKeyValue kv : Iterables.concat(familyMap.values())){
			TKeyValue newTKV = new TKeyValue();
			newTKV.setKey(new TCellKey().setFamily(kv.getFamily()).setQualifier(kv.getQualifier()));
			newTKV.setValue(kv.getValue());
			newTPut.addToValues(newTKV);
		}
		newTMutation.setPut(newTPut);
		return newTMutation;
	}
}
