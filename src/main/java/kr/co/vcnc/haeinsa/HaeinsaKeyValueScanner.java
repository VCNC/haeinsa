package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.Comparator;

import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import com.google.common.collect.ComparisonChain;

public interface HaeinsaKeyValueScanner {
	public static final Comparator<HaeinsaKeyValueScanner> COMPARATOR = new Comparator<HaeinsaKeyValueScanner>() {
		
		@Override
		public int compare(HaeinsaKeyValueScanner o1, HaeinsaKeyValueScanner o2) {
			return ComparisonChain.start()
					.compare(o1.peek(), o2.peek(), HaeinsaKeyValue.COMPARATOR)
					.compare(o1.getSequenceID(), o2.getSequenceID())
					.result();
		}
	}; 
	
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
	
	public TRowLock peekLock() throws IOException;

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
