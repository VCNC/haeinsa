package kr.co.vcnc.haeinsa;

public interface HaeinsaDeleteTracker {
	void add(HaeinsaKeyValue kv, long sequenceID);
	
	boolean isDeleted(HaeinsaKeyValue kv, long sequenceID);
	
	public void reset();
}
