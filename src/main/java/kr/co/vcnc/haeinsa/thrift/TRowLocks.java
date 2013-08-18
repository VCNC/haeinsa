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
package kr.co.vcnc.haeinsa.thrift;

import static kr.co.vcnc.haeinsa.HaeinsaConstants.ROW_LOCK_VERSION;

import java.io.IOException;
import java.util.Arrays;

import kr.co.vcnc.haeinsa.thrift.generated.TRowKey;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLockState;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import com.google.common.base.Preconditions;

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
	
	public static boolean isPrimary(TRowLock rowLock) {
		return !rowLock.isSetPrimary();
	}
	
	public static boolean isSecondaryOf(TRowLock primaryRowLock, TRowKey secondaryRowKey, TRowLock secondaryRowLock) {
		return primaryRowLock.getCommitTimestamp() == secondaryRowLock.getCommitTimestamp()
				&& containRowKeyAsSecondary(primaryRowLock, secondaryRowKey);
	}
	
	/**
	 * Check if given rowLock has secondaryRowKey in secondaries.
	 *
	 * @param primaryRowLock given RowLock
	 * @param secondaryRowKey secondaryRowKey to check if given RowLock contains as secondary.
	 * @return true if given rowLock contains given secondaryRowKey as secondary.
	 */
	public static boolean containRowKeyAsSecondary(TRowLock primaryRowLock, TRowKey secondaryRowKey) {
		// Check if secondaries of rowLock contains secondaryRowKey as element.
		if (primaryRowLock.isSetSecondaries()) {
			for (TRowKey rowKey : primaryRowLock.getSecondaries()) {
				boolean match = rowKey != null
						&& rowKey.isSetTableName()
						&& secondaryRowKey.isSetTableName()
						&& Arrays.equals(rowKey.getTableName(), secondaryRowKey.getTableName())
						&& rowKey.isSetRow()
						&& secondaryRowKey.isSetRow()
						&& Arrays.equals(rowKey.getRow(), secondaryRowKey.getRow());
				if (match) {
					return true;
				}
			}
		}
		return false;
	}
}
