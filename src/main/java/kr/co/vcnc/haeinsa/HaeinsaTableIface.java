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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Wrapper of HTableInterface for Haeinsa. Some of methods in
 * {@link HTableInterface} are dropped because of implementing complexity. Most
 * of methods which directly access to DB now need {@link HaeinsaTransaction} as
 * an argument which supervises transaction.
 * <p>
 * Implemented by {@link HaeinsaTable}.
 */
public interface HaeinsaTableIface extends Closeable {

    /**
     * Gets the name of this table.
     *
     * @return the table name.
     */
    byte[] getTableName();

    /**
     * Returns the {@link Configuration} object used by this instance.
     * <p>
     * The reference returned is not a copy, so any change made to it will
     * affect this instance.
     */
    Configuration getConfiguration();

    /**
     * Gets the {@link HTableDescriptor table descriptor} for this table.
     *
     * @throws IOException if a remote or network exception occurs.
     */
    HTableDescriptor getTableDescriptor() throws IOException;

    /**
     * Extracts certain cells from a given row.
     *
     * @param tx HaeinsaTransaction which this operation is participated in.
     * It can be null if user don't want to execute get inside transaction.
     * @param get The object that specifies what data to fetch and from which row.
     * @return The data coming from the specified row, if it exists. If the row
     * specified doesn't exist, the {@link HaeinsaResult} instance returned
     * won't contain any {@link HaeinsaKeyValue}, as indicated by
     * {@link HaeinsaResult#isEmpty()}.
     * @throws IOException if a remote or network exception occurs.
     */
    HaeinsaResult get(@Nullable HaeinsaTransaction tx, HaeinsaGet get) throws IOException;

    /**
     * Gets a inter-row scanner on the current table for the given family.
     * Similar with {@link HaeinsaTableIface#getScanner(HaeinsaTransaction, HaeinsaScan)}.
     *
     * @param tx HaeinsaTransaction which this operation is participated in.
     * It can be null if user don't want to execute scan inside transaction.
     * @param family The column family to scan.
     * @return A scanner.
     * @throws IOException if a remote or network exception occurs.
     */
    HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family) throws IOException;

    /**
     * Gets a inter-row scanner on the current table for the given family and qualifier.
     * Similar with {@link HaeinsaTableIface#getScanner(HaeinsaTransaction, HaeinsaScan)}.
     *
     * @param tx HaeinsaTransaction which this operation is participated in.
     * It can be null if user don't want to execute scan inside transaction.
     * @param family The column family to scan.
     * @param qualifier The column qualifier to scan.
     * @return A scanner.
     * @throws IOException if a remote or network exception occurs.
     */
    HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, byte[] family, byte[] qualifier)
            throws IOException;

    /**
     * Returns a inter-row scanner on the current table as specified by the {@link HaeinsaScan}
     * object.
     *
     * @param tx HaeinsaTransaction which this operation is participated in.
     * It can be null if user don't want to execute scan inside transaction.
     * @param scan A configured {@link HaeinsaScan} object.
     * @return A scanner.
     * @throws IOException if a remote or network exception occurs.
     */
    HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, HaeinsaScan scan) throws IOException;

    /**
     * Returns a intra-row scanner on the current table as specified by the {@link HaeinsaIntraScan}
     * object.
     *
     * @param tx HaeinsaTransaction which this operation is participated in.
     * It can be null if user don't want to execute scan inside transaction.
     * @param intraScan A configured {@link HaeinsaIntraScan} object.
     * @return A scanner.
     * @throws IOException if a remote or network exception occurs.
     */
    HaeinsaResultScanner getScanner(@Nullable HaeinsaTransaction tx, HaeinsaIntraScan intraScan) throws IOException;

    /**
     * Puts some data in the table.
     * <p>
     * The update is buffered in client until tx is commited.
     *
     * @param put The data to put.
     * @throws IOException if a remote or network exception occurs.
     */
    void put(HaeinsaTransaction tx, HaeinsaPut put) throws IOException;

    /**
     * Puts some data in the table, in batch.
     * <p>
     * The updates are buffered in client until tx is commited.
     *
     * @param puts The list of Puts to apply.
     * @throws IOException if a remote or network exception occurs.
     */
    void put(HaeinsaTransaction tx, List<HaeinsaPut> puts) throws IOException;

    /**
     * Deletes the specified cells/row.
     * <p>
     * The update is buffered in client until tx is commited.
     *
     * @param delete The object that specifies what to delete.
     * @throws IOException if a remote or network exception occurs.
     */
    void delete(HaeinsaTransaction tx, HaeinsaDelete delete) throws IOException;

    /**
     * Deletes the specified cells/rows in bulk.
     * <p>
     * The update is buffered in client until tx is commited.
     *
     * @param deletes List of things to delete.
     * @throws IOException if a remote or network exception occurs.
     */
    void delete(HaeinsaTransaction tx, List<HaeinsaDelete> deletes) throws IOException;

    /**
     * Releases any resources related to table.
     *
     * @throws IOException if a remote or network exception occurs.
     */
    @Override
    void close() throws IOException;
}
