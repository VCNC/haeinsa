package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import kr.co.vcnc.haeinsa.thrift.generated.TMutation;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;

import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Iterables;

/**
 * 
 * @author Myungbo Kim
 *
 */
public abstract class HaeinsaMutation {
	protected byte[] row = null;
	protected Map<byte[], NavigableSet<HaeinsaKeyValue>> familyMap = new TreeMap<byte[], NavigableSet<HaeinsaKeyValue>>(
			Bytes.BYTES_COMPARATOR);


	/**
	 * Method for retrieving the put's familyMap
	 * 
	 * @return familyMap
	 */
	public Map<byte[], NavigableSet<HaeinsaKeyValue>> getFamilyMap() {
		return this.familyMap;
	}

	/**
	 * Method for setting the put's familyMap
	 */
	public void setFamilyMap(Map<byte[], NavigableSet<HaeinsaKeyValue>> map) {
		this.familyMap = map;
	}
	
	public Set<byte[]> getFamilies(){
		return familyMap.keySet();
	}

	/**
	 * Method to check if the familyMap is empty
	 * 
	 * @return true if empty, false otherwise
	 */
	public boolean isEmpty() {
		return familyMap.isEmpty();
	}

	/**
	 * Method for retrieving the delete's row
	 * 
	 * @return row
	 */
	public byte[] getRow() {
		return this.row;
	}

	public int compareTo(final Row d) {
		return Bytes.compareTo(this.getRow(), d.getRow());
	}
	
	public abstract void add(HaeinsaMutation newMutation);
	
	/**
	 * Change HaeinsaMutation to TMutation (Thrift Class).
	 * <p> TMutation contains list of either TPut or TRemove.
	 * @return TMutation (Thrift class)
	 */
	public abstract TMutation toTMutation();
	
	public HaeinsaKeyValueScanner getScanner(final long sequenceID){
		return new MutationScanner(sequenceID);
	}
	
	/**
	 * 
	 * @author Myungbo Kim
	 *
	 */
	private class MutationScanner implements HaeinsaKeyValueScanner {
		private final long sequenceID;
		private final Iterator<HaeinsaKeyValue> iterator;
		private HaeinsaKeyValue current;
		
		public MutationScanner(long sequenceID){
			this.sequenceID = sequenceID;
			this.iterator = Iterables.concat(getFamilyMap().values()).iterator();
		}

		@Override
		public HaeinsaKeyValue peek() {
			if (current != null){
				return current;
			}
			if (iterator.hasNext()){
				current = iterator.next();
			}
			return current;
		}

		@Override
		public HaeinsaKeyValue next() throws IOException {
			HaeinsaKeyValue result = peek();
			current = null;
			return result;
		}

		@Override
		public long getSequenceID() {
			return sequenceID;
		}
		
		@Override
		public TRowLock peekLock() throws IOException {
			return null;
		}

		@Override
		public void close() {
			
		}
	}
}
