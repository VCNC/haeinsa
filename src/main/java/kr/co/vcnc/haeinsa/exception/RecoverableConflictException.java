/**
 * Copyright (C) 2013 VCNC, inc
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

/**
 * This exception will occur when transaction fails on making row state to
 * stable. Since failure of the operation can be recovered by next access, this
 * exception can be ignored. This class extends {@link ConflictException},
 * because this occurs on failure of write by other transaction.
 */
public class RecoverableConflictException extends ConflictException {

    private static final long serialVersionUID = 3720142235607540830L;

    public RecoverableConflictException() {
    }

    public RecoverableConflictException(String message) {
        super(message);
    }

    public RecoverableConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
