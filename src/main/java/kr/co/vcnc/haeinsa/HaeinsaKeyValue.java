package kr.co.vcnc.haeinsa;

import java.util.Comparator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ComparisonChain;

public class HaeinsaKeyValue {
	public static final Comparator<HaeinsaKeyValue> COMPARATOR =new Comparator<HaeinsaKeyValue>() {
		@Override
		public int compare(HaeinsaKeyValue o1, HaeinsaKeyValue o2) {
			return ComparisonChain.start()
					.compareFalseFirst(o1.getRow() != null, o2.getRow() != null)
					.compare(o1.getRow(), o2.getRow(), Bytes.BYTES_COMPARATOR)
					.compareFalseFirst(o1.getFamily() != null , o2.getFamily() != null)
					.compare(o1.getFamily(), o2.getFamily(), Bytes.BYTES_COMPARATOR)
					.compareFalseFirst(o1.getQualifier() != null, o2.getQualifier() != null)
					.compare(o1.getQualifier(), o2.getQualifier(), Bytes.BYTES_COMPARATOR)
					.compare(o1.getType(), o2.getType())
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
}
