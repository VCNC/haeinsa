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
 * {@link TMutation} (Thrift class) 에 대응하는 abstract class 이다.
 * 단일한 row 에 대한 여러 Put 혹은 Delete 를 한꺼번에 들고 있을 수 있다. 
 * {@link HaeinsaPut} 혹은 {@link HaeinsaDelete} 로 구현해서 사용한다.
 * 
 * <p>HaeinsaTable 이 put/delete 정보를 get/scan 에 projection 할 때 사용할 수 있도록 {@link HaeinsaKeyValueScanner} 를 return 해준다. 
 * @author Youngmok Kim
 *
 */
public abstract class HaeinsaMutation {
	protected byte[] row = null;
	//	{ family -> HaeinsaKeyValue }
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
	 * <p> TMutation contains list of either TPut or TRemove. (not both) 
	 * @return TMutation (Thrift class)
	 */
	public abstract TMutation toTMutation();
	
	/**
	 * 단일한 row 에 대한 {@link #MutationScanner} 를 반환하게 된다. 
	 * @param sequenceID sequence id represent which Scanner is newer one. Lower id is newer one.
	 * @return
	 */
	public HaeinsaKeyValueScanner getScanner(final long sequenceID){
		return new MutationScanner(sequenceID);
	}
	
	/**
	 * HaeinsaMutation 가 가지고 있는 Put 혹은 Delete Type 의 HaeinsaKeyValue 를 모아서 
	 * HaeinsaKeyValueScanner interface로 접근할 수 있게 해준다.
	 * <p>MutationScanner 가 제공하는 iterator 는 {@link HaeinsaKeyValue#COMPARATOR} 에 의해서 정렬된 
	 * HaeinsaKeyValue 의 Collection 에 접근하게 된다.
	 * <p>하나의 MutationScanner 가 제공하는 값들은 동일한 sequenceID 를 가지게 된다.
	 * @author Myungbo Kim
	 *
	 */
	private class MutationScanner implements HaeinsaKeyValueScanner {
		private final long sequenceID;
		private final Iterator<HaeinsaKeyValue> iterator;
		private HaeinsaKeyValue current;
		
		/**
		 * Iterator provided by MutationScanner access to sorted list of HaeinsaKeyValue by {@link HaeinsaKeyValue#COMPARATOR}.
		 * <p>The part of generating iterator in this constructor based on the assumption that 
		 * values() function of TreeMap return sorted collection of values.
		 * Otherwise, the way to generate iterator should be changed.
		 * @param sequenceID sequence id represent which Scanner is newer one. Lower id is newer one.
		 */
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
