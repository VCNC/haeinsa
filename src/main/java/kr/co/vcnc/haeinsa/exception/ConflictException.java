package kr.co.vcnc.haeinsa.exception;

import java.io.IOException;

/**
 * 
 * @author Myungbo Kim
 *
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
