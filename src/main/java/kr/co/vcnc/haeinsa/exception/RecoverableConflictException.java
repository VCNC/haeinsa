package kr.co.vcnc.haeinsa.exception;

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
