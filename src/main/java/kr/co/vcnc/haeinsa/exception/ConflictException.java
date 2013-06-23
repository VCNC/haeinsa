package kr.co.vcnc.haeinsa.exception;

import java.io.IOException;

/**
 *	Exception when transaction meet conflict during execution.
 *	This exception extends {@link IOException}, so user don't need to 
 *	explicitly distinguish between failure on HBase IO and conflict. 
 */
public class ConflictException extends IOException {

	/**
	 *
	 */
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
