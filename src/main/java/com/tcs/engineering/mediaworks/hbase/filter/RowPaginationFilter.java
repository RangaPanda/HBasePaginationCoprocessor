package com.tcs.engineering.mediaworks.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationOffsetProtoBuff;

/**
 * A filter, based on the PageFilter, takes two arguments: limit and offset.
 * This filter can be used for specified row count, in order to efficient
 * lookups and paginated results for end users.
 * 
 * This filter can be used when regions are only one.
 * @author Ranga Panda, ranga.panda@gmail.com
 */
public class RowPaginationFilter extends FilterBase {

	
	private long rowsAccepted = 0;
	private int limit = 0;
	private long offset = 0;

	/**
	 * Constructor that takes a maximum page size.
	 * get row from offset to offset+limit ( offset<= row<=offset+limit )
	 * @param offset start position
	 * @param limit count from offset position
	 */
	public RowPaginationFilter( int limit,  long offset,long rowsAccepted) {
		this.limit = limit;
		this.offset = offset;
		this.rowsAccepted = rowsAccepted;
	}
	
	@Override
    public void reset() {
            // noop
    }
	
    @Override
    public boolean filterAllRemaining() {
            return this.rowsAccepted > this.limit+this.offset;
    }
    
    
    @Override
    public boolean filterRowKey(byte[] rowKey, int offset, int length) {
            return false;
    }
   
    
	@Override
	public ReturnCode filterKeyValue(Cell ignored) throws IOException {
	   return ReturnCode.INCLUDE;
	}
  
    //true to exclude row, false to include row.
    @Override
    public boolean filterRow() {            
            boolean isExclude = this.rowsAccepted < this.offset || this.rowsAccepted>=this.limit+this.offset;
            rowsAccepted++;
            return isExclude;
    }
	 
    
	  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments)
		 {
		   int limit = ParseFilter.convertByteArrayToInt((byte[])filterArguments.get(0));
		   long offset = ParseFilter.convertByteArrayToLong((byte[])filterArguments.get(1));
		   long rowsAcceptedVal = ParseFilter.convertByteArrayToLong((byte[])filterArguments.get(1));
		  
		   return new RowPaginationFilter(limit,offset,rowsAcceptedVal);
		 }

	  @Override
	  public byte[] toByteArray() {
		
		   PaginationOffsetProtoBuff.PaginationOffsetRequest.Builder builder = PaginationOffsetProtoBuff.PaginationOffsetRequest.newBuilder();
		   builder.setLimit(this.limit);
		   builder.setOffset(this.offset);
		   builder.setRowsAccepted(this.rowsAccepted);
		   
		   return builder.build().toByteArray();
		 }

	  
	  public static RowPaginationFilter parseFrom(byte[] pbBytes)
			   throws DeserializationException {
		  
		        PaginationOffsetProtoBuff.PaginationOffsetRequest proto;
				 
			   try {
				   
			     proto = PaginationOffsetProtoBuff.PaginationOffsetRequest.parseFrom(pbBytes);
			     
			   } catch (InvalidProtocolBufferException e) {
			     throw new DeserializationException(e);
			   }
			   
		   return new RowPaginationFilter(proto.getLimit(),proto.getOffset(),proto.getRowsAccepted());
	 }

	

	  @Override
	  public String toString() {
	    return this.getClass().getSimpleName() + " Limit :" + this.limit+" offset :"+this.offset;
	  }

	 	
    
}
