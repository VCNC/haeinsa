package kr.co.vcnc.haeinsa;

import java.nio.ByteBuffer;
import java.util.NavigableSet;

import kr.co.vcnc.haeinsa.thrift.generated.TCellKey;
import kr.co.vcnc.haeinsa.thrift.generated.TMutation;
import kr.co.vcnc.haeinsa.thrift.generated.TMutationType;
import kr.co.vcnc.haeinsa.thrift.generated.TRemove;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Implementation of {@link HaeinsaMuation} which only contains HaeinsaKeyValue with 
 * {@link Type#DeleteFamily} and {@link Type#DeleteColumn} identifier.
 * HaeinsaPut can be analogous to {@link Delete} class in HBase. 
 * <p>HaeinsaDelete only contains data of single row.
 * @author Myungbo Kim
 *
 */
public class HaeinsaDelete extends HaeinsaMutation {
	
	public HaeinsaDelete(byte[] row) {
		this.row = row;
	}

	/**
	 * @param d
	 *            Delete to clone.
	 */
	public HaeinsaDelete(final HaeinsaDelete d) {
		this.row = d.getRow();
		this.familyMap.putAll(d.getFamilyMap());
	}

	/**
	 * Delete all versions of all columns of the specified family.
	 * <p>
	 * Overrides previous calls to deleteColumn and deleteColumns for the
	 * specified family.
	 * 
	 * @param family
	 *            family name
	 * @return this for invocation chaining
	 */
	public HaeinsaDelete deleteFamily(byte[] family) {
		NavigableSet<HaeinsaKeyValue> set = familyMap.get(family);
		if (set == null) {
			set = Sets.newTreeSet(HaeinsaKeyValue.COMPARATOR);
		} else if (!set.isEmpty()) {
			set.clear();
		}
		set.add(new HaeinsaKeyValue(row, family, null, null,
				KeyValue.Type.DeleteFamily));
		familyMap.put(family, set);
		return this;
	}

	/**
	 * Delete all versions of the specified column.
	 * 
	 * @param family
	 *            family name
	 * @param qualifier
	 *            column qualifier
	 * @return this for invocation chaining
	 */
	public HaeinsaDelete deleteColumns(byte[] family, byte[] qualifier) {
		NavigableSet<HaeinsaKeyValue> set = familyMap.get(family);
		if (set == null) {
			set = Sets.newTreeSet(HaeinsaKeyValue.COMPARATOR);
		}
		set.add(new HaeinsaKeyValue(this.row, family, qualifier, null,
				KeyValue.Type.DeleteColumn));
		familyMap.put(family, set);
		return this;
	}

	/**
	 * Merge all familyMap to this instance.
	 * @throw IllegalStateException if newMuatation is not HaeinsaDelete
	 */
	@Override
	public void add(HaeinsaMutation newMutation) {
		Preconditions.checkState(newMutation instanceof HaeinsaDelete);
		for (HaeinsaKeyValue newKV : Iterables.concat(newMutation.getFamilyMap().values())){
			if (newKV.getType() == KeyValue.Type.DeleteFamily){
				deleteFamily(newKV.getFamily());
			}else{
				deleteColumns(newKV.getFamily(), newKV.getQualifier());
			}
		}
		
	}
		
	@Override
	public TMutation toTMutation() {
		TMutation newTMutation = new TMutation(TMutationType.REMOVE);
		TRemove newTRemove = new TRemove();
		for (HaeinsaKeyValue kv : Iterables.concat(familyMap.values())){
			switch (kv.getType()) {
			case DeleteColumn:{
				newTRemove.addToRemoveCells(new TCellKey().setFamily(kv.getFamily()).setQualifier(kv.getQualifier()));
				break;
			}
			
			case DeleteFamily:{
				newTRemove.addToRemoveFamilies(ByteBuffer.wrap(kv.getFamily()));
				break;
			}

			default:
				break;
			}
		}
		newTMutation.setRemove(newTRemove);
		return newTMutation;
	}
}
