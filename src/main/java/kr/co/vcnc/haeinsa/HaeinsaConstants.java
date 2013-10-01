/**
 * Copyright (C) 2013 VCNC Inc.
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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Static Class of Constants for Haeinsa
 */
public final class HaeinsaConstants {
    private HaeinsaConstants() {
    }

    /**
     * Haeinsa protocol version of this release.
     * Version field {@link TRowLock#version} is included in every {@link TRowLock},
     * to support backward compatibility in the future.
     * There is only one version (=1) until now.
     */
    public static final int ROW_LOCK_VERSION = 1;

    /**
     * Timeout duration of {@link TRowLock} on single transaction.
     * If {@link TRowLockState} of primary row does not become
     * {@link TRowLockState#STABLE} until this timeout, other client can abort this lock.
     * <p>
     * This timeout should be bigger than sum of maximum timeskew between Haeinsa clients and
     * execution time of most transactions.
     * There would be lot of aborted transactions by other clients otherwise.
     */
    public static final long ROW_LOCK_TIMEOUT = TimeUnit.SECONDS.toMillis(10);

    /**
     * This is default name of lock column family used by Haeinsa transaction.
     * The column family will save {@link TRowLock} to allow transaction.
     * All HBase table use Haeinsa should have column family which have same name with this.
     * In this release, user can't control name of lock column family.
     */
    public static final byte[] LOCK_FAMILY = Bytes.toBytes("!lock!");

    /**
     * This is default name of column qualifier which save {@link TRowLock} inside lock column family.
     * The qualifier should be accessed only by Haeinsa client library, not by user code.
     * In this release, user can't control name of lock qualifier.
     */
    public static final byte[] LOCK_QUALIFIER = Bytes.toBytes("lock");

    public static final int RECOVER_MAX_RETRY_COUNT = 3;
}
