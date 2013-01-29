package kr.co.vcnc.haeinsa;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;

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
		this.deleteFamily(family, HConstants.LATEST_TIMESTAMP);
		return this;
	}

	/**
	 * Delete all columns of the specified family with a timestamp less than or
	 * equal to the specified timestamp.
	 * <p>
	 * Overrides previous calls to deleteColumn and deleteColumns for the
	 * specified family.
	 * 
	 * @param family
	 *            family name
	 * @param timestamp
	 *            maximum version timestamp
	 * @return this for invocation chaining
	 */
	private HaeinsaDelete deleteFamily(byte[] family, long timestamp) {
		List<KeyValue> list = familyMap.get(family);
		if (list == null) {
			list = new ArrayList<KeyValue>();
		} else if (!list.isEmpty()) {
			list.clear();
		}
		list.add(new KeyValue(row, family, null, timestamp,
				KeyValue.Type.DeleteFamily));
		familyMap.put(family, list);
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
		this.deleteColumns(family, qualifier, HConstants.LATEST_TIMESTAMP);
		return this;
	}

	/**
	 * Delete all versions of the specified column with a timestamp less than or
	 * equal to the specified timestamp.
	 * 
	 * @param family
	 *            family name
	 * @param qualifier
	 *            column qualifier
	 * @param timestamp
	 *            maximum version timestamp
	 * @return this for invocation chaining
	 */
	private HaeinsaDelete deleteColumns(byte[] family, byte[] qualifier,
			long timestamp) {
		List<KeyValue> list = familyMap.get(family);
		if (list == null) {
			list = new ArrayList<KeyValue>();
		}
		list.add(new KeyValue(this.row, family, qualifier, timestamp,
				KeyValue.Type.DeleteColumn));
		familyMap.put(family, list);
		return this;
	}
	
	@Override
	public HaeinsaKeyValueScanner getScanner(byte[] family) {
		return null;
	}
}
