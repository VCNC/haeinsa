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

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;

import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Contains Transaction information of single row. This information is only
 * saved in client memory until {@link HaeinsaTransaction#commit()} called.
 */
class HaeinsaRowTransaction {
    // current RowLock saved in HBase. null if there is no lock at all.
    private TRowLock current;
    // mutations will be saved in order of executions.
    // If this rowTransaction is created during recovering failed transaction by other client,
    // following mutations variable is empty.
    private final List<HaeinsaMutation> mutations = Lists.newArrayList();
    private final HaeinsaTableTransaction tableTransaction;

    HaeinsaRowTransaction(HaeinsaTableTransaction tableTransaction) {
        this.tableTransaction = tableTransaction;
    }

    public TRowLock getCurrent() {
        return current;
    }

    public void setCurrent(TRowLock current) {
        this.current = current;
    }

    public List<HaeinsaMutation> getMutations() {
        return mutations;
    }

    public int getIterationCount() {
        if (mutations.size() > 0) {
            if (mutations.get(0) instanceof HaeinsaPut) {
                return mutations.size();
            } else {
                return mutations.size() + 1;
            }
        }
        return 1;
    }

    public void addMutation(HaeinsaMutation mutation) {
        if (mutations.size() <= 0) {
            mutations.add(mutation);
        } else {
            HaeinsaMutation lastMutation = mutations.get(mutations.size() - 1);
            if (lastMutation.getClass() != mutation.getClass()) {
                if (mutation.getClass() == HaeinsaPut.class) {
                    mergeHaeinsaPutIfPossible((HaeinsaPut) mutation);
                } else if (mutation.getClass() == HaeinsaDelete.class) {
                    mergeHaeinsaDeleteIfPossible((HaeinsaDelete) mutation);
                } else {
                    // not going to happen before any other type of HaeinsaMutation added.
                    mutations.add(mutation);
                }
            } else {
                lastMutation.add(mutation);
            }
        }
    }

    // Put, Delete + Put => (Put + filteredPut), eliminatedDelete, leftPut
    private void mergeHaeinsaPutIfPossible(HaeinsaPut put) {
        if (mutations.size() <= 1) {
            mutations.add(put);
            return;
        }

        HaeinsaDelete lastDelete = (HaeinsaDelete) mutations.remove(mutations.size() - 1);
        HaeinsaPut filteredPut = new HaeinsaPut(put.row);
        HaeinsaDelete eliminatedDelete = new HaeinsaDelete(lastDelete.row);
        HaeinsaPut leftPut = new HaeinsaPut(put.row);

        HaeinsaDeleteTracker familyDeleteTracker = new HaeinsaDeleteTracker();
        NavigableMap<byte[], NavigableMap<byte[], HaeinsaKeyValue>> columnKVs = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

        for (Entry<byte[], NavigableSet<HaeinsaKeyValue>> entry : lastDelete.familyMap.entrySet()) {
            for (HaeinsaKeyValue haeinsaKV : entry.getValue()) {
                switch (haeinsaKV.getType()) {
                case DeleteColumn: {
                    NavigableMap<byte[], HaeinsaKeyValue> cells = columnKVs.get(haeinsaKV.getFamily());
                    if (cells == null) {
                        cells = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                        columnKVs.put(haeinsaKV.getFamily(), cells);
                    }
                    cells.put(haeinsaKV.getQualifier(), haeinsaKV);
                    break;
                }
                case DeleteFamily: {
                    familyDeleteTracker.add(haeinsaKV, 1);
                    eliminatedDelete.deleteFamily(haeinsaKV.getFamily());
                    break;
                }
                default: {
                    break;
                }
                }
            }
        }

        for (Entry<byte[], NavigableSet<HaeinsaKeyValue>> entry : put.familyMap.entrySet()) {
            for (HaeinsaKeyValue haeinsaKV : entry.getValue()) {
                if (!familyDeleteTracker.isDeleted(haeinsaKV, 0)) {
                    NavigableMap<byte[], HaeinsaKeyValue> cells = columnKVs.get(haeinsaKV.getFamily());
                    if (cells == null) {
                        cells = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
                        columnKVs.put(haeinsaKV.getFamily(), cells);
                    }
                    cells.put(haeinsaKV.getQualifier(), haeinsaKV);
                } else {
                    filteredPut.add(haeinsaKV.getFamily(), haeinsaKV.getQualifier(), haeinsaKV.getValue());
                }
            }
        }

        for (Entry<byte[], NavigableMap<byte[], HaeinsaKeyValue>> families : columnKVs.entrySet()) {
            for (Entry<byte[], HaeinsaKeyValue> entry : families.getValue().entrySet()) {
                HaeinsaKeyValue kv = entry.getValue();
                switch (kv.getType()) {
                case DeleteColumn: {
                    eliminatedDelete.deleteColumns(kv.getFamily(), kv.getQualifier());
                    break;
                }
                case Put: {
                    leftPut.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
                    break;
                }
                default: {
                    break;
                }
                }
            }
        }

        mutations.get(mutations.size() - 1).add(filteredPut);
        if (!eliminatedDelete.isEmpty()) {
            mutations.add(eliminatedDelete);
            if (!leftPut.isEmpty()) {
                mutations.add(leftPut);
            }
        } else {
            if (!leftPut.isEmpty()) {
                mutations.get(mutations.size() - 1).add(leftPut);
            }
        }
    }

    // Delete, Put + Delete => mergedDelete, filteredPut
    private void mergeHaeinsaDeleteIfPossible(HaeinsaDelete delete) {
        if (mutations.size() <= 1) {
            mutations.add(delete);
            return;
        }

        HaeinsaPut lastPut = (HaeinsaPut) mutations.remove(mutations.size() - 1);
        HaeinsaPut filteredPut = new HaeinsaPut(lastPut.row);

        HaeinsaDeleteTracker deleteTracker = new HaeinsaDeleteTracker();
        for (Entry<byte[], NavigableSet<HaeinsaKeyValue>> entry : delete.familyMap.entrySet()) {
            for (HaeinsaKeyValue haeinsaKV : entry.getValue()) {
                deleteTracker.add(haeinsaKV, 1);
            }
        }

        for (Entry<byte[], NavigableSet<HaeinsaKeyValue>> entry : lastPut.familyMap.entrySet()) {
            for (HaeinsaKeyValue haeinsaKV : entry.getValue()) {
                if (!deleteTracker.isDeleted(haeinsaKV, 0)) {
                    filteredPut.add(haeinsaKV.getFamily(), haeinsaKV.getQualifier(), haeinsaKV.getValue());
                }
            }
        }

        mutations.get(mutations.size() - 1).add(delete);
        if (!filteredPut.isEmpty()) {
            // do not add HaeinsaPut if no HaeinsaKeyValue survived.
            mutations.add(filteredPut);
        }
    }

    public HaeinsaTableTransaction getTableTransaction() {
        return tableTransaction;
    }

    /**
     * Return list of {@link HaeinsaKeyValueScanner}s which wrap mutations
     * (Put and Delete) contained inside instance. Also assign sequenceID to every
     * MutationScanner of {@link HaeinsaMutation}.
     */
    public List<HaeinsaKeyValueScanner> getScanners() {
        List<HaeinsaKeyValueScanner> result = Lists.newArrayList();
        for (int i = 0; i < mutations.size(); i++) {
            HaeinsaMutation mutation = mutations.get(i);
            result.add(mutation.getScanner(mutations.size() - i));
        }
        return result;
    }
}
