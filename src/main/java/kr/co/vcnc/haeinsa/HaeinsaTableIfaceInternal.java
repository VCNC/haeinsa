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

import kr.co.vcnc.haeinsa.exception.ConflictException;
import kr.co.vcnc.haeinsa.thrift.generated.TMutationType;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Extended interface from {@link HaeinsaTableIface} which defines private methods that are used to implement transaction.
 */
interface HaeinsaTableIfaceInternal extends HaeinsaTableIface {

    /**
     * Commit single row put only Transaction. Directly change
     * {@link TRowLockState} from {@link TRowLockState#STABLE} to
     * {@link TRowLockState#STABLE} and increase commitTimestamp by 1. Separate
     * this because Single Row put only transaction can save one checkAndPut
     * operation to complete.
     * <p>
     * If TRowLock is changed and checkAndPut failed, it means transaction is
     * failed so throw {@link ConflictException}.
     *
     * @throws IOException ConflictException, HBase IOException.
     */
    void commitSingleRowPutOnly(HaeinsaRowTransaction rowState, byte[] row) throws IOException;

    /**
     * Read {@link TRowLock} from HBase and compare that lock with prevRowLock.
     * If TRowLock is changed, it means transaction is failed, so throw
     * {@link ConflictException}.
     *
     * @throws IOException ConflictException, HBase IOException.
     * @throws NullPointException if oldLock is null (haven't read lock from
     * HBase)
     */
    void checkSingleRowLock(HaeinsaRowTransaction rowState, byte[] row) throws IOException;

    /**
     * Prewrite specific row with rowState variable.
     * Put version, state, commitTimestamp, currentTimestamp fields of {@link TRowLock} to lock column of the row on HBase.
     * If first mutation in rowState is {@link HaeinsaPut}, then apply consequent Puts in the same RPC which changes lock to
     * {@link TRowLockState#PREWRITTEN}.
     * Remaining mutations which is not applied with first RPC is remained in {@link TRowLock#mutations}.
     * This field will be used in {@link #applyMutations()} stage.
     * Columns written in prewritten stage will be recorded in {@link TRowLock#prewritten} field,
     * which will be used in {@link HaeinsaTransaction#recover(boolean)} to clean up dirty data
     * if transaction failed.
     * <p>
     * Add list of secondary rows in secondaries field if this row is primary row, add key of primary row in primary field otherwise.
     *
     * @throws IOException ConflictException, HBase IOException
     */
    void prewrite(HaeinsaRowTransaction rowState, byte[] row, boolean isPrimary) throws IOException;

    /**
     * Apply all remained mutations to HBase row while {@link TRowLock} is in {@link TRowLockState#PREWRITTEN}.
     * <p>
     * Haeinsa groups consequent Puts or Removes to one HaeinsaMutation,
     * and {@link TRowLock#mutations} save them in alternative order.
     * Apply Puts and Removes alternatively as same order which those mutations happen during transaction.
     * {@link TRowLock} in HBase row is changed only when applying Puts.
     * <p>
     * So if transaction failed just after applying {@link TMutationType#REMOVE} to HBase,
     * increased {@link TRowLock#currentTimestamp} and remaining {@link TRowLock#mutations} fields are not changed on HBase.
     * If other client try to recover this transaction, currentTimestamp is smaller by 1 than actual currentTimestamp when transaction failed.
     * However this is not an issue for transaction consistency because new client will execute idempotent remove operations one more time
     * on same timestamp which are already used during previous transaction attempt.
     *
     * @throws IOException ConflictException, HBase IOException.
     */
    void applyMutations(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

    /**
     * Change specific row to {@link TRowLockState#STABLE} state.
     * Use commitTimestamp field of {@link TRowLock} as timestamp on HBase.
     * Only {@link TRowLock#version}, {@link TRowLock#state} and {@link TRowLock#commitTimestamp} fields are written.
     *
     * @throws IOException ConflictException, HBase IOException.
     */
    void makeStable(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

    /**
     * Change specific row to {@link TRowLockState#COMMITTED} state.
     * While normal transaction execution, if this method is called {@link TRowLock} is changed from
     * {@link TRowLockState#PREWRITTEN} to {@link TRowLockState#COMMITTED}.
     * If transaction failed after commit primary by this method,
     * failed-over client will call this method again to extend lock expiry on primary row.
     * In this case, {@link TRowLock} is remained in {@link TRowLockState#COMMITTED}.
     *
     * @throws IOException ConflictException, HBase IOException.
     */
    void commitPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

    /**
     * get {@link TRowLock} from HBase. This method never returns null.
     *
     * @param row row
     * @return row lock
     * @throws IOException HBase IOException.
     */
    TRowLock getRowLock(byte[] row) throws IOException;

    /**
     * Change {@link TRowLock} to {@link TRowLockState#ABORTED} state to roll back
     * failed or expired transaction to previous state when transaction have not started.
     * <p>
     * {@link TRowLock} can transform from {@link TRowLockState#PREWRITTEN} state to {@link TRowLockState#ABORTED},
     * or {@link TRowLockState#ABORTED} to another {@link TRowLockState#ABORTED}.
     * Later is when different client failed again during cleaning up aborted transaction.
     *
     * @throws IOException ConflictException, HBase IOException.
     */
    void abortPrimary(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;

    /**
     * Delete columns that are prewritten to the specific row during prewritten stage.
     * Haeinsa can infer prewritten columns to clean up by parsing prewritten field in {@link TRowLock}.
     * Should remove column which have {@link TRowLock#currentTimestamp} as timestamp.
     * This is possible by using {@link Delete#deleteColumn()} instead of {@link Delete#deleteColumns()}.
     * <p>
     * Because Haeinsa uses {@link HTableInterface#checkAndDelete()} to delete prewrittens,
     * {@link TRowLock} is not changed. It will throw {@link ConflictException} if failed to acquire lock in checkAndDelete.
     *
     * @throws IOException ConflictException, HBase IOException.
     */
    void deletePrewritten(HaeinsaRowTransaction rowTxState, byte[] row) throws IOException;
}
