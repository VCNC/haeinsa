/**
 * Copyright (C) 2013-2014 VCNC Inc.
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

import java.util.HashMap;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * A HaeinsaTransactionLocal stores values associated with {@link HaeinsaTransaction}.
 * <p>
 * You can create transaction local using {@link HaeinsaTransactionLocal#newLocal()}.
 */
public final class HaeinsaTransactionLocal<T> {
    private HaeinsaTransactionLocal() {}

    /*
     * Creates a transaction local variable.
     * @return a new transaction local variable
     */
    public static <T> HaeinsaTransactionLocal<T> newLocal() {
        return new HaeinsaTransactionLocal<T>();
    }

    /*
     * Returns the value associated with the specified transaction.
     * If no value is set, returns <code>null</code>.
     * @param tx the transaction for which the variable it to be retrieved
     * @return the specified transaction's value of this transaction local,
     * or <code>null</code> if no value is set
     */
    @Nullable
    public T get(HaeinsaTransaction tx) {
        Preconditions.checkNotNull(tx);
        return (T) tx.getLocals().get(this);
    }

    /*
     * Sets the value associated with the specified transaction.
     * Storing <code>null</code> as a value is not allowed.
     * @param tx the the transaction for which the value will be set
     * @param value the value to be associated with the transaction (cannot be <code>null</code>)
     */
    public void set(HaeinsaTransaction tx, T value) {
        Preconditions.checkNotNull(tx);
        Preconditions.checkNotNull(value);
        tx.getLocals().set(this, value);
    }

    /*
     * Returns <code>true</code> if this transaction local variable has the value associated with
     * the specified transaction.
     * @param tx the transaction for which the variable it to be retrieved
     * @return <code>true</code> if this transaction local variable has the value associated with
     * the specified transaction
     */
    public boolean isSet(HaeinsaTransaction tx) {
        Preconditions.checkNotNull(tx);
        return tx.getLocals().isSet(this);
    }

    /*
     * Removes the value associated with the specified transaction.
     * @param tx the the transaction for which the value will be removed
     * @return the previous value associated with transaction, or null if there was no value
     */
    public T remove(HaeinsaTransaction tx) {
        Preconditions.checkNotNull(tx);
        return (T) tx.getLocals().remove(this);
    }

    static class HaeinsaTransactionLocals {
        @SuppressWarnings("rawtypes")
        private final HashMap localMap;

        HaeinsaTransactionLocals() {
            localMap = Maps.newHashMap();
        }

        @SuppressWarnings("unchecked")
        public <T> T get(HaeinsaTransactionLocal<T> local) {
            return (T) localMap.get(local);
        }

        @SuppressWarnings("unchecked")
        public <T> void set(HaeinsaTransactionLocal<T> local, T value) {
            localMap.put(local, value);
        }

        public <T> boolean isSet(HaeinsaTransactionLocal<T> local) {
            return localMap.containsKey(local);
        }

        @SuppressWarnings("unchecked")
        public <T> T remove(HaeinsaTransactionLocal<T> local) {
            return (T) localMap.remove(local);
        }
    }
}
