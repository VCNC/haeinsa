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
import java.util.Map;
import java.util.NavigableSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import com.google.common.collect.Lists;

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
                if (mutation instanceof HaeinsaPut) {
                    mergeHaeinsaPutIfPossible((HaeinsaPut) mutation);
                } else if (mutation instanceof HaeinsaDelete) {
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

    /**
     * When new HaeinsaPut added at the end of mutations which is finished by consequent Put and
     * Delete, new HaeinsaPut could be partially merged with previous HaeinsaPut. At the same time,
     * last HaeinsaDelete could lose some HaeinsaKeyValues because new HaeinsaPut could overwrite
     * HaeinsaDelete on specific column. This method will do this merging process.
     */
    private void mergeHaeinsaPutIfPossible(HaeinsaPut put) {
        if (mutations.size() <= 1) {
            mutations.add(put);
            return;
        }
        Preconditions.checkArgument(mutations.get(mutations.size() - 2) instanceof HaeinsaPut);

        HaeinsaDelete lastDelete = (HaeinsaDelete) mutations.remove(mutations.size() - 1);
        FilterResult filterResult = filter(lastDelete, put);
        mutations.get(mutations.size() - 1).add(filterResult.getRemained());
        mutations.add(lastDelete);
        if (!filterResult.getDeleted().isEmpty()) {
            mutations.add(filterResult.getDeleted());
        }
    }

    /**
     * When new HaeinsaDelete added at the end of mutations which is finished by consequent Delete
     * and Put, new HaeinsaDelete can be merged with previous HeainsaDelete while HaeinsaPut loses
     * some HaeinsaKeyValues which are deleted by added HaeinsaDelete. This method will do this
     * merging process.
     */
    private void mergeHaeinsaDeleteIfPossible(HaeinsaDelete delete) {
        if (mutations.size() <= 1) {
            mutations.add(delete);
            return;
        }
        Preconditions.checkArgument(mutations.get(mutations.size() - 2) instanceof HaeinsaDelete);

        // prevDelete, lastPut + newDelete => (prevDelete + newDelete), remainedPut
        HaeinsaPut lastPut = (HaeinsaPut) mutations.remove(mutations.size() - 1);
        FilterResult filterResult = filter(delete, lastPut);
        mutations.get(mutations.size() - 1).add(delete);
        if (!filterResult.getRemained().isEmpty()) {
            mutations.add(filterResult.getRemained());
        }
    }

    @VisibleForTesting
    static FilterResult filter(HaeinsaDelete delete, HaeinsaPut put) {
        HaeinsaDeleteTracker deleteTracker = new HaeinsaDeleteTracker();
        deleteTracker.add(delete, 0);
        FilterResult filterResult = new FilterResult(delete.getRow());
        for (Map.Entry<byte[], NavigableSet<HaeinsaKeyValue>> entry : put.getFamilyMap().entrySet()) {
            for (HaeinsaKeyValue keyValue : entry.getValue()) {
                if (deleteTracker.isDeleted(keyValue)) {
                    filterResult.getDeleted().add(keyValue.getFamily(), keyValue.getQualifier(), keyValue.getValue());
                } else {
                    filterResult.getRemained().add(keyValue.getFamily(), keyValue.getQualifier(), keyValue.getValue());
                }
            }
        }
        return filterResult;
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

    @VisibleForTesting
    static final class FilterResult {
        private final HaeinsaPut remained;
        private final HaeinsaPut deleted;

        private FilterResult(byte[] row) {
            this.remained = new HaeinsaPut(row);
            this.deleted = new HaeinsaPut(row);
        }

        public HaeinsaPut getRemained() {
            return remained;
        }

        public HaeinsaPut getDeleted() {
            return deleted;
        }
    }
}
