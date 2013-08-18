package kr.co.vcnc.haeinsa;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory of HaeinsaTable
 */
public interface HaeinsaTableIfaceFactory {

	/**
	 * Creates a new HaeinsaTableIface.
	 *
	 * @param config HBaseConfiguration instance.
	 * @param tableName name of the HBase table.
	 * @return HaeinsaTableIface instance.
	 */
	HaeinsaTableIface createHaeinsaTableIface(Configuration config, byte[] tableName);

	/**
	 * Release the HaeinsaTable resource represented by the table.
	 * @param table
	 */
	void releaseHaeinsaTableIface(final HaeinsaTableIface table) throws IOException;

}
