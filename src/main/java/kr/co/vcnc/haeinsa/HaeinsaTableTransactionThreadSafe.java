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

import org.apache.hadoop.hbase.util.Bytes;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by ehud on 8/17/15
 * Extends .{@link HaeinsaTableTransaction} and added the ability to call createOrGetRowState from multi threads
 * <p>
 * It have map of {byte[] row -> {@link HaeinsaRowTransaction} and reference to
 * {@link HaeinsaTransactionThreadSafe}
 */
public class HaeinsaTableTransactionThreadSafe extends HaeinsaTableTransaction {

    HaeinsaTableTransactionThreadSafe(HaeinsaTransaction transaction) {
        super(transaction);
    }

    /**
     * overrides {@link HaeinsaTableTransaction} in order to be thread safe.
     * @return RowTransaction - {@link HaeinsaRowTransaction} which contained in
     * this instance.
     */
    @Override
    public HaeinsaRowTransaction createOrGetRowState(byte[] row) {
        HaeinsaRowTransaction rowState = rowStates.get(row);
        if (rowState == null) {
            synchronized (rowStates){
                rowState = rowStates.get(row);
                if (rowState == null) {
                    rowState = new HaeinsaRowTransactionThreadSafe(this);
                    rowStates.put(row, rowState);
                }
            }
        }
        return rowState;
    }

    protected NavigableMap<byte[], HaeinsaRowTransaction> createHaeinsaRowTransactionNavigableMap() {
        return new ConcurrentSkipListMap<byte[], HaeinsaRowTransaction>(Bytes.BYTES_COMPARATOR);
    }
}
