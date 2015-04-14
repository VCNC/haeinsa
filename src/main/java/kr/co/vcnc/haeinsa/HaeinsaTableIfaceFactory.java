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
     */
    void releaseHaeinsaTableIface(final HaeinsaTableIface table) throws IOException;
}
