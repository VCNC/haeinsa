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
 * Exception when row's lock in transaction isn't expired yet during execution.
 * This exception extends {@link ConflictException}
 */
public class NotExpiredYetException extends ConflictException {

    private static final long serialVersionUID = -5160271558362505568L;

    public NotExpiredYetException() {
    }

    public NotExpiredYetException(String message) {
        super(message);
    }

    public NotExpiredYetException(String message, Throwable cause) {
        super(message, cause);
    }
}
