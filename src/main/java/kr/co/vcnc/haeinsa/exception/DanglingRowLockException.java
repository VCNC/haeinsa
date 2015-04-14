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
package kr.co.vcnc.haeinsa.exception;

import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;

/**
 * Exception when transaction meet dangling RowLock during execution. RowLock is
 * in dangling if the RowLock is secondary lock and the primary of the RowLock
 * doesn't have the RowLock as secondary. Dangling row can be retrieve with
 * {@link #getDanglingRowKey}
 * <p>
 * Dangling rowLock should not be appear normal case, but we throw this
 * exception if there is fatal consistency error in Haeinsa.
 */
public class DanglingRowLockException extends ConflictException {
    private static final long serialVersionUID = -9220580990865263679L;

    private final TRowKey danglingRowKey;

    public DanglingRowLockException(TRowKey danglingRowKey) {
        super();
        this.danglingRowKey = danglingRowKey;
    }

    public DanglingRowLockException(TRowKey danglingRowKey, String message) {
        super(message);
        this.danglingRowKey = danglingRowKey;
    }

    /**
     * This method returns {@link TRowKey} of Dangling row. You can get dangling
     * rowLock with this row key.
     *
     * @return {@link TRowKey} of Dangling row.
     */
    public TRowKey getDanglingRowKey() {
        return danglingRowKey;
    }
}
