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
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import kr.co.vcnc.haeinsa.thrift.generated.TMutation;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * HaeinsaMutation is abstract class equivalent to auto-generated thrift class {@link TMutation}.
 * HaeinsaMutation can handle multiple Puts or Deletes of single row,
 * but single HaeinsaMutation instance can not represent both at same time.
 * {@link HaeinsaPut} and {@link HaeinsaDelete} are implementations of this class.
 * <p>
 * This class assists HaeinsaTable to project history of Puts/Deletes inside transaction
 * to Get/Scan operations which is executed after.
 * These late Get/Scan operations can see the mutated view of specific row
 * even before those mutations are committed to HBase.
 * <p>
 * HaeinsaMutation provides {@link HaeinsaKeyValueScanner} interface by {@link #getScanner(long)} method.
 */
public abstract class HaeinsaMutation extends HaeinsaOperation {
    protected byte[] row;
    // { family -> HaeinsaKeyValue }
    protected Map<byte[], NavigableSet<HaeinsaKeyValue>> familyMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

    /**
     * Method for retrieving the put's familyMap
     *
     * @return familyMap
     */
    public Map<byte[], NavigableSet<HaeinsaKeyValue>> getFamilyMap() {
        return this.familyMap;
    }

    /**
     * Method for setting the put's familyMap
     */
    public void setFamilyMap(Map<byte[], NavigableSet<HaeinsaKeyValue>> map) {
        this.familyMap = map;
    }

    public Set<byte[]> getFamilies() {
        return familyMap.keySet();
    }

    /**
     * Method to check if the familyMap is empty
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return familyMap.isEmpty();
    }

    /**
     * Method for retrieving the delete's row
     *
     * @return row
     */
    public byte[] getRow() {
        return this.row;
    }

    public int compareTo(final Row d) {
        return Bytes.compareTo(this.getRow(), d.getRow());
    }

    public abstract void add(HaeinsaMutation newMutation);

    /**
     * Change HaeinsaMutation to TMutation (Thrift Class).
     * <p>
     * TMutation contains list of either TPut or TRemove. (not both)
     *
     * @return TMutation (Thrift class)
     */
    public abstract TMutation toTMutation();

    /**
     * Return {@link HaeinsaKeyValueScanner} interface for single row.
     *
     * @param sequenceID sequence id represent which Scanner is newer one. Lower
     * id is newer one.
     */
    public HaeinsaKeyValueScanner getScanner(final long sequenceID) {
        return new MutationScanner(sequenceID);
    }

    /**
     * MutationScanner is HaeinsaKeyValueScanner implementation which provides interface to access
     * actual Puts or Deletes which inherit HaeinsaMutations.
     * <p>
     * Iterator provided by MutationScanner uses {@link HaeinsaKeyValue#COMPARATOR} to
     * sort HaeinsaKeyValue in {@link HaeinsaMutation#familyMap}.
     * <p>
     * All HaeinsaKeyValue provided by single MutationScanner have same sequenceID.
     */
    private class MutationScanner implements HaeinsaKeyValueScanner {
        private final long sequenceID;
        private final Iterator<HaeinsaKeyValue> iterator;
        private HaeinsaKeyValue current;

        /**
         * Iterator provided by MutationScanner access to sorted list of
         * HaeinsaKeyValue by {@link HaeinsaKeyValue#COMPARATOR}.
         * <p>
         * The part of generating iterator in this constructor based on the
         * assumption that values() function of TreeMap return sorted collection
         * of values. Otherwise, the way to generate iterator should be changed.
         *
         * @param sequenceID sequence id represent which Scanner is newer one.
         * Lower id is newer one.
         */
        public MutationScanner(long sequenceID) {
            this.sequenceID = sequenceID;
            this.iterator = Iterables.concat(getFamilyMap().values()).iterator();
        }

        @Override
        public HaeinsaKeyValue peek() {
            if (current != null) {
                return current;
            }
            if (iterator.hasNext()) {
                current = iterator.next();
            }
            return current;
        }

        @Override
        public HaeinsaKeyValue next() throws IOException {
            HaeinsaKeyValue result = peek();
            current = null;
            return result;
        }

        @Override
        public long getSequenceID() {
            return sequenceID;
        }

        @Override
        public TRowLock peekLock() throws IOException {
            return null;
        }

        @Override
        public void close() {}
    }
}
