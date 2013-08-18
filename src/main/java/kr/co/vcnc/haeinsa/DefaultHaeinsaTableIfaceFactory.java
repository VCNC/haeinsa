package kr.co.vcnc.haeinsa;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

/**
 * Default HaeinsaTableIfaceFactory
 */
public class DefaultHaeinsaTableIfaceFactory implements HaeinsaTableIfaceFactory {
	private final HTableInterfaceFactory tableInterfaceFactory;

	public DefaultHaeinsaTableIfaceFactory(HTableInterfaceFactory tableInterfaceFactory) {
		this.tableInterfaceFactory = tableInterfaceFactory;
	}

	@Override
	public HaeinsaTableIface createHaeinsaTableIface(Configuration config, byte[] tableName){
		return new HaeinsaTable(tableInterfaceFactory.createHTableInterface(config, tableName));
	}

	@Override
	public void releaseHaeinsaTableIface(HaeinsaTableIface table) throws IOException {
		table.close();
	}
}
