package kr.co.vcnc.haeinsa.thrift;

import static kr.co.vcnc.haeinsa.Constants.ROW_LOCK_VERSION;

import java.io.IOException;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public final class HaeinsaThriftUtils {
	private static final TProtocolFactory PROTOCOL_FACTORY = new TCompactProtocol.Factory();
	
	private static TSerializer createSerializer(){
		return new TSerializer(PROTOCOL_FACTORY);
	}
	
	private static TDeserializer createDeserializer(){
		return new TDeserializer(PROTOCOL_FACTORY);
	}
	
	public static RowLock deserialize(byte[] rowLockBytes) throws IOException {
		if (rowLockBytes == null){
			return new RowLock(ROW_LOCK_VERSION, RowState.STABLE, Long.MIN_VALUE);
		}
		RowLock rowLock = new RowLock();
		TDeserializer deserializer = createDeserializer();
		try{
			deserializer.deserialize(rowLock, rowLockBytes);
			return rowLock;
		}catch (TException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
	
	public static byte[] serialize(RowLock rowLock) throws IOException {
		if (rowLock.getCommitTimestamp() == Long.MIN_VALUE){
			return null;
		}
		TSerializer serializer = createSerializer();
		try{
			return serializer.serialize(rowLock);
		}catch (TException e) {
			throw new IOException(e.getMessage(), e);
		}
	}
}
