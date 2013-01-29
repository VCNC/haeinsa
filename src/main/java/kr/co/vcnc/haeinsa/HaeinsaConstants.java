package kr.co.vcnc.haeinsa;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.util.Bytes;

public final class HaeinsaConstants {
	private HaeinsaConstants(){}
	
	static {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		calendar.set(2100, 0, 1);
		ROW_LOCK_MIN_TIMESTAMP = calendar.getTimeInMillis();
	}
	
	public static final int ROW_LOCK_VERSION = 1;
	public static final long ROW_LOCK_MIN_TIMESTAMP;
	public static final long ROW_LOCK_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
	
	public static final byte[] LOCK_FAMILY = Bytes.toBytes("__lock__");
	public static final byte[] LOCK_QUALIFIER = Bytes.toBytes("lock");
}
