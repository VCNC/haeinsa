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
 * Created by ehud on 8/17/15.
 * Representation of single transaction in Haeinsa. extends {@link HaeinsaTransaction}
 * It contains {@link HaeinsaTableTransactionThreadSafe}s to include information of overall transaction,
 * and have reference to {@link HaeinsaTransactionManager} which created this instance.
 * <p>
 * ThreadSafeHaeinsaTransaction can be generated via calling {@link HaeinsaTransactionManager#begin()}
 * or {@link HaeinsaTransactionManager#getTransaction(byte[], byte[])}.
 * Former is used when start new transaction, later is used when try to roll back or retry failed transaction.
 * <p>
 * One {@link HaeinsaTransactionThreadSafe} can't be used after calling {@link #commit()} or {@link #rollback()} is called.
 */
public class HaeinsaTransactionThreadSafe extends HaeinsaTransaction {

    public HaeinsaTransactionThreadSafe(HaeinsaTransactionManager manager) {
        super(manager);
    }

    @Override
    protected HaeinsaTableTransaction createOrGetTableState(byte[] tableName) {
        HaeinsaTableTransaction tableTxState = txStates.getTableStates().get(tableName);
        if (tableTxState == null) {
            synchronized (txStates){
                tableTxState = txStates.getTableStates().get(tableName);
                if (tableTxState == null) {
                    tableTxState = new HaeinsaTableTransactionThreadSafe(this);
                    txStates.getTableStates().put(tableName, tableTxState);
                }
            }
        }
        return tableTxState;
    }

    @Override
    protected HaeinsaTransactionState createTransactionState() {
        return new ThreadSafeHaeinsaTransactionState();
    }

    protected static class ThreadSafeHaeinsaTransactionState extends HaeinsaTransactionState{

        @Override
        protected NavigableMap<byte[], HaeinsaTableTransaction> createTablesStates() {
            return new ConcurrentSkipListMap<byte[], HaeinsaTableTransaction>(Bytes.BYTES_COMPARATOR);
        }
    }
}
