package kr.co.vcnc.haeinsa;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HaeinsaGet can be analogous to {@link Get} class in HBase. 
 * <p>HaeinsaGet only contains data of single row.
 * @author Youngmok Kim
 *
 */
public class HaeinsaGet {
	private byte[] row = null;
	private Map<byte[], NavigableSet<byte[]>> familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(
			Bytes.BYTES_COMPARATOR);

	/**
	 * Create a Get operation for the specified row.
	 * <p>
	 * If no further operations are done, this will get the latest version of
	 * all columns in all families of the specified row.
	 * 
	 * @param row
	 *            row key
	 */
	public HaeinsaGet(byte[] row) {
		this.row = row;
	}

	/**
	 * Get all columns from the specified family.
	 * <p>
	 * Overrides previous calls to addColumn for this family.
	 * 
	 * @param family
	 *            family name
	 * @return the Get object
	 */
	public HaeinsaGet addFamily(byte[] family) {
		familyMap.remove(family);
		familyMap.put(family, null);
		return this;
	}

	/**
	 * Get the column from the specific family with the specified qualifier.
	 * <p>
	 * Overrides previous calls to addFamily for this family.
	 * 
	 * @param family
	 *            family name
	 * @param qualifier
	 *            column qualifier
	 * @return the Get objec
	 */
	public HaeinsaGet addColumn(byte[] family, byte[] qualifier) {
		NavigableSet<byte[]> set = familyMap.get(family);
		if (set == null) {
			set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		}
		set.add(qualifier);
		familyMap.put(family, set);
		return this;
	}

	/**
	 * Method for retrieving the get's row
	 * 
	 * @return row
	 */
	public byte[] getRow() {
		return this.row;
	}

	/**
	 * Method for retrieving the get's familyMap
	 * 
	 * @return familyMap
	 */
	public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
		return this.familyMap;
	}
}
