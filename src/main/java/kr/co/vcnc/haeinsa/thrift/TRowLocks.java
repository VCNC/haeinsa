package kr.co.vcnc.haeinsa.thrift;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_VERSION;

import java.io.IOException;

import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * Static class for TRowLock (Thrift class) Provide static method to
 * serialize/deserialize with TCompactProtocol of Thrift
 * <p>
 * TRowLock(commitTimestamp = Long.MIN_VALUE) <=> byte[] null
 */
public final class TRowLocks {
	private static final TProtocolFactory PROTOCOL_FACTORY = new TCompactProtocol.Factory();

	private static TSerializer createSerializer() {
		return new TSerializer(PROTOCOL_FACTORY);
	}

	private static TDeserializer createDeserializer() {
		return new TDeserializer(PROTOCOL_FACTORY);
	}

	private TRowLocks() {}

	public static TRowLock deserialize(byte[] rowLockBytes) throws IOException {
		if (rowLockBytes == null) {
			return new TRowLock(ROW_LOCK_VERSION, TRowLockState.STABLE, Long.MIN_VALUE);
		}
		TRowLock rowLock = new TRowLock();
		TDeserializer deserializer = createDeserializer();
		try {
			deserializer.deserialize(rowLock, rowLockBytes);
			return rowLock;
		} catch (TException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public static byte[] serialize(TRowLock rowLock) throws IOException {
		if (rowLock.getCommitTimestamp() == Long.MIN_VALUE) {
			return null;
		}
		TSerializer serializer = createSerializer();
		try {
			return serializer.serialize(rowLock);
		} catch (TException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
}
