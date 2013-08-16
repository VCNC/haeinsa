package kr.co.vcnc.haeinsa.exception;

public class RecoverableConflictException extends ConflictException {

	public RecoverableConflictException() {
	}

	public RecoverableConflictException(String message) {
		super(message);
	}

	public RecoverableConflictException(String message, Throwable cause) {
		super(message, cause);
	}
}
