package kr.co.vcnc.haeinsa;

import java.util.List;

public interface HaeinsaResult {
	byte[] getRow();
	List<HaeinsaKeyValue> list();
	byte[] getValue(byte[] family, byte[] qualifier);
	boolean containsColumn(byte[] family, byte[] qualifier);
	boolean isEmpty();
}
