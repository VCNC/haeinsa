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

import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Custom integration of {@link ColumnRangeFilter} in Haeinsa. In contrast to
 * {@link HaeinsaScan}, HaeinsaIntraScan can be used to retrieve range of column
 * qualifier inside single row with scan-like way.
 * <p>
 * User can specify column family, range of qualifier, size of batch at a time
 * and whether start column and last column are included.
 * <p>
 * Default batch size is 32.
 */
public class HaeinsaIntraScan extends HaeinsaQuery {
    private final byte[] row;
    private final byte[] minColumn;
    private final boolean minColumnInclusive;
    private final byte[] maxColumn;
    private final boolean maxColumnInclusive;
    private int batch = 32;

    // if this set is empty, then scan every family
    private final NavigableSet<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    public HaeinsaIntraScan(final byte[] row,
            final byte[] minColumn, boolean minColumnInclusive,
            final byte[] maxColumn, boolean maxColumnInclusive) {
        this.row = row;
        this.minColumn = minColumn;
        this.minColumnInclusive = minColumnInclusive;
        this.maxColumn = maxColumn;
        this.maxColumnInclusive = maxColumnInclusive;
    }

    public byte[] getMaxColumn() {
        return maxColumn;
    }

    public byte[] getMinColumn() {
        return minColumn;
    }

    public byte[] getRow() {
        return row;
    }

    public boolean isMinColumnInclusive() {
        return minColumnInclusive;
    }

    public boolean isMaxColumnInclusive() {
        return maxColumnInclusive;
    }

    public HaeinsaIntraScan addFamily(byte[] family) {
        families.add(family);
        return this;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    public int getBatch() {
        return batch;
    }

    public NavigableSet<byte[]> getFamilies() {
        return families;
    }

    @Override
    public HaeinsaIntraScan setFilter(Filter filter) {
        super.setFilter(filter);
        return this;
    }
}
