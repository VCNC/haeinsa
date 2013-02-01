package kr.co.vcnc.haeinsa;

import java.io.IOException;

public interface HaeinsaKeyValueScanner {
	
	/**
	 * Look at the next KeyValue in this scanner, but do not iterate scanner.
	 * 
	 * @return the next KeyValue
	 */
	public HaeinsaKeyValue peek();

	/**
	 * Return the next KeyValue in this scanner, iterating the scanner
	 * 
	 * @return the next KeyValue
	 */
	public HaeinsaKeyValue next() throws IOException;

	/**
	 * Get the sequence id associated with this KeyValueScanner. This is
	 * required for comparing multiple files to find out which one has the
	 * latest data. The default implementation for this would be to return 0. A
	 * file having lower sequence id will be considered to be the older one.
	 */
	public long getSequenceID();

	/**
	 * Close the KeyValue scanner.
	 */
	public void close();
}
