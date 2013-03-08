package kr.co.vcnc.haeinsa;

import java.util.Comparator;

import kr.co.vcnc.haeinsa.utils.NullableComparator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * Modified POJO container of {@link KeyValue} class in HBase.
 * Like {@link KeyValue}, contains only one Key-Value data.
 * 
 * <p>HaeinsaKeyValue contains row, family, qualifier, value information and Type information, but not timestamp.
 * Because haeinsa use timestamp for version control, user cannot manually control timestamp of HaeinsaKeyValue. 
 * Type is same Enum with {@link org.apache.hadoop.hbase.KeyValue.Type}.
 * 
 * <p>HaeinsaKeyValue has public static comparator which can be used in navigableMap.
 * This comparator is ComparisionChain of {@link NullableCompator} wrapped {@link org.apache.hadoop.hbase.util.Bytes#BYTES_COMPARATOR}.
 * The order of comparisonChain is row, family, qualifier, value and type.
 * 
 * @author Myungbo Kim
 *
 */
public class HaeinsaKeyValue {
	public static final Comparator<HaeinsaKeyValue> COMPARATOR =new Comparator<HaeinsaKeyValue>() {
		@Override
		public int compare(HaeinsaKeyValue o1, HaeinsaKeyValue o2) {
			return ComparisonChain.start()
					.compare(o1.getRow(), o2.getRow(), new NullableComparator<byte[]>(Bytes.BYTES_COMPARATOR))
					.compare(o1.getFamily(), o2.getFamily(), new NullableComparator<byte[]>(Bytes.BYTES_COMPARATOR))
					.compare(o1.getQualifier(), o2.getQualifier(), new NullableComparator<byte[]>(Bytes.BYTES_COMPARATOR))
					.compare(o2.getType(), o1.getType())
					.result();
		}
	};
	
	private byte[] row;
	private byte[] family;
	private byte[] qualifier;
	private byte[] value;
	private Type type;
	
	public HaeinsaKeyValue(){
	}
	
	public HaeinsaKeyValue(KeyValue keyValue){
		this(keyValue.getRow(), keyValue.getFamily(), keyValue.getQualifier(), keyValue.getValue(), KeyValue.Type.codeToType(keyValue.getType()));
	}
	
	public HaeinsaKeyValue(byte[] row, byte[] family, byte[] qualifier, byte[] value, Type type){
		this.row = row;
		this.family = family;
		this.qualifier = qualifier;
		this.value = value;
		this.type = type;
	}
	
	public byte[] getRow() {
		return row;
	}
	
	public void setRow(byte[] row) {
		this.row = row;
	}

	public byte[] getFamily() {
		return family;
	}

	public void setFamily(byte[] family) {
		this.family = family;
	}

	public byte[] getQualifier() {
		return qualifier;
	}

	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
	
	/**
	 * for debugging
	 */
	@Override
	public String toString(){
		return Objects.toStringHelper(this.getClass())
				.add("row", Bytes.toStringBinary(row))
	            .add("family", Bytes.toStringBinary(family))
	            .add("qualifier", Bytes.toStringBinary(qualifier))
	            .add("value", Bytes.toStringBinary(value))
	            .add("type", type)
	            .toString();
	}
}
