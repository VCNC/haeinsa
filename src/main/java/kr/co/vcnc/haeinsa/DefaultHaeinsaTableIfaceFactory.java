/**
 * Copyright (C) 2013-2015 VCNC Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    public HaeinsaTableIface createHaeinsaTableIface(Configuration config, byte[] tableName) {
        return new HaeinsaTable(tableInterfaceFactory.createHTableInterface(config, tableName));
    }

    @Override
    public void releaseHaeinsaTableIface(HaeinsaTableIface table) throws IOException {
        table.close();
    }
}
