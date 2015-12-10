package com.tcs.engineering.mediaworks.hbase.pojo;

import org.apache.hadoop.hbase.util.Bytes;

public class CellInfo implements Comparable<CellInfo>{

	private byte[] rowKey;
	private byte[] cf;
	private byte[] qualifier;
	private byte[] value;
	
	
	public byte[] getRowKey() {
		return rowKey;
	}
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}
	public byte[] getCf() {
		return cf;
	}
	public void setCf(byte[] cf) {
		this.cf = cf;
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
	public int compareTo(CellInfo obj) {
		
		if(obj !=null ){
			return  Bytes.compareTo(this.rowKey,obj.getRowKey());
		}
		
		return 0;
	}
	
	@Override
	public String toString() {
		return "CellInfo [rowKey=" + Bytes.toString(rowKey) + ", cf="
				+ Bytes.toString(cf) + ", qualifier="
				+ Bytes.toString(qualifier) + ", value="
				+ Bytes.toString(value) + "]";
	}
	
	
	
	
}
