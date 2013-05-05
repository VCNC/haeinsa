package kr.co.vcnc.haeinsa;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Static Class of Constants for Haeinsa
 */
public final class HaeinsaConstants {
	private HaeinsaConstants() { }

	/**
	 * 향후에 Haeinsa 의 버젼이 올라갈 경우 하위 호완을 지원하기 위해서 TRowLock 에 버젼 정보를 포함해야 한다.
	 * 현재 존재하는 Haeinsa 의 버젼은 1로 유일하다.
	 */
	public static final int ROW_LOCK_VERSION = 1;

	/**
	 * TRowLock 은 5초보다 길게 유지될 수 없다.
	 * 만약 이 시간 이후에도 TRowLockState 가 Stable 이 되지 않는다면 다른 Transaction 이 해당 TRowLock 을 Abort 시킬 수 있다.
	 * 이 시간은 Haeinsa Client 간의 TimeSkew 와 Transaction 이 완료되는데 걸리는 시간의 합보다 커야한다.
	 * 그렇지 않다면 지속적으로 한 쪽 Client 가 다른 Client 를 Abort 시킬 수 있다.
	 */
	public static final long ROW_LOCK_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

	/**
	 * Haeinsa 에서 TRowLock 을 저장하기 위한 ColumnFamily 의 이름을 저장하고 있다. 기본값은 "!lock!" 이다.
	 * Haeinsa 를 사용하기 위해서는 해당 테이블이 !lock! 이라는 이름의 ColumnFamily 를 가지고 있어야 한다.
	 */
	public static final byte[] LOCK_FAMILY = Bytes.toBytes("!lock!");

	/**
	 * Haeinsa 에서 TRowLock 을 저장하기 위한 ColumnQualifier 의 이름을 저장하고 있다. 기본값은 "lock" 이다.
	 * 이 ColumnFamily 에 저장되는 값은 Haeinsa Client 의 내부 library 에 의해서만 생성/제거 되어야만 한다.
	 */
	public static final byte[] LOCK_QUALIFIER = Bytes.toBytes("lock");
}
