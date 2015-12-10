package com.tcs.engineering.mediaworks.hbase.pojo;

public class RegionInfoPojo implements Comparable<RegionInfoPojo>{

	private String startKey;
	private String endKey;
	private long count;
	private long recordStart;
	private long recordEnd;
	
	private long rowsAccepted = 0;
	private int limit = 0;
	private long offset = 0;
	
	
	public String getStartKey() {
		return startKey;
	}
	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}
	public String getEndKey() {
		return endKey;
	}
	public void setEndKey(String endKey) {
		this.endKey = endKey;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	
	public long getRecordStart() {
		return recordStart;
	}
	public void setRecordStart(long recordStart) {
		this.recordStart = recordStart;
	}
	public long getRecordEnd() {
		return recordEnd;
	}
	public void setRecordEnd(long recordEnd) {
		this.recordEnd = recordEnd;
	}
		
	public long getRowsAccepted() {
		return rowsAccepted;
	}
	public void setRowsAccepted(long rowsAccepted) {
		this.rowsAccepted = rowsAccepted;
	}
	public int getLimit() {
		return limit;
	}
	public void setLimit(int limit) {
		this.limit = limit;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	public int compareTo(RegionInfoPojo obj) {
		 
		if(obj !=null ){
			return this.getStartKey().compareToIgnoreCase(obj.getStartKey());
		}
		return 0;
	}
	
	
	@Override
	public String toString() {
		return "RegionInfoPojo [startKey=" + startKey + ", endKey=" + endKey
				+ ", count=" + count + ", recordStart=" + recordStart
				+ ", recordEnd=" + recordEnd + ", rowsAccepted=" + rowsAccepted
				+ ", limit=" + limit + ", offset=" + offset + "]";
	}
	
	
}
