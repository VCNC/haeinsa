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

import java.io.IOException;

/**
 * Exception when transaction meet conflict during execution.
 * This exception extends {@link IOException}, so user don't need to
 * explicitly distinguish between failure on HBase IO and conflict.
 */
public class ConflictException extends IOException {

	private static final long serialVersionUID = -6181950952954013762L;

	public ConflictException() {
		super();
	}

	public ConflictException(String message) {
		super(message);
	}

	public ConflictException(String message, Throwable cause) {
		super(message, cause);
	}
}
