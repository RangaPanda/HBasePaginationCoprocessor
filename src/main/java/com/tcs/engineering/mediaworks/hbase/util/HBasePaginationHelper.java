package com.tcs.engineering.mediaworks.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import com.tcs.engineering.mediaworks.hbase.filter.RowPaginationFilter;
import com.tcs.engineering.mediaworks.hbase.pojo.CellInfo;
import com.tcs.engineering.mediaworks.hbase.pojo.RegionInfoPojo;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationCoprocessorService;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationInput;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationOutput;
/**
 * HBase Pagination Helper class, this class will be used to scan the rows based on regions
 * @author Ranga Panda, ranga.panda@gmail.com
 *
 */
public class HBasePaginationHelper {

	  /**
	   * Perform the Pagination based on offset and limit
	   * @param connection
	   * @param tableName
	   * @param cf
	   * @param qualifier
	   * @param FilterList filter
	   * @param offset
	   * @param limit
	   * @return
	   * @throws Throwable
	   */
	public static List<CellInfo> doScan(Connection connection,String tableName,String cf,String qualifier,
			FilterList filter,long offset,int limit) throws Throwable{
		
		 
		 List<CellInfo> finalResult=null;
		 List<RegionInfoPojo> regInfo= getRegionInfo(connection, tableName, cf, qualifier);
		 
		 if(regInfo.size()>0){
			 
			 regInfo= getScanInfo(regInfo, limit, offset);
			 
			 if(regInfo != null && regInfo.size()>0){
				  		
				 
				 finalResult= new ArrayList<CellInfo>();
				 
				 for(RegionInfoPojo infoPojo:regInfo){
					 
					try (Table table = connection.getTable(TableName.valueOf(tableName))){
						
					
					 Scan scan = new Scan(Bytes.toBytes(infoPojo.getStartKey()),Bytes.toBytes(infoPojo.getEndKey()));
					 
					  if(qualifier!=null && qualifier.length()>0 
					    		&& cf!=null && cf.length()>0){
							scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
					    }else if(cf!=null && cf.length()>0){
					    	scan.addFamily(Bytes.toBytes(cf));
					    }
					 
					  RowPaginationFilter rowPagFilter=new RowPaginationFilter(infoPojo.getLimit(), infoPojo.getOffset(),infoPojo.getRowsAccepted());
					  
					  if(filter==null){
						 scan.setFilter(rowPagFilter);
					  	  
					  }else{
						  
						 filter.addFilter(rowPagFilter);
						 scan.setFilter(filter);
					  }
					  
					  ResultScanner scanner = table.getScanner(scan);
					  
					  CellInfo info=null;
					  for (Result result : scanner) {
						
						  if(! result.isEmpty()){
							  
							  for(Cell cell:result.listCells()){
								  
								  info= new CellInfo();
								  info.setQualifier(CellUtil.cloneQualifier(cell));
								  info.setRowKey(CellUtil.cloneRow(cell));
								  info.setValue(CellUtil.cloneValue(cell));
								  info.setCf(CellUtil.cloneFamily(cell));
								  
								  finalResult.add(info);
								
							  }
						  }
					  } // scanner
				   }// try
			   } // RegionInfoPojo loop
			 }
		 
		 }else{
			 
			 return null;
		 }
				
		return finalResult;
	}
	
	
	 
	/**
	 * Perform the Pagination based on offset and  limit
	 * @param connection
	 * @param tableName
	 * @param cf
	 * @param qualifier
	 * @param startkey
	 * @param endkey
	 * @param filter
	 * @param offset
	 * @param limit
	 * @return
	 * @throws Throwable
	 */
	public static List<CellInfo> doScan(Connection connection,String tableName,String cf,String qualifier,
			String startkey,String endkey,FilterList filter,long offset,int limit) throws Throwable{
		
		 
		 List<CellInfo> finalResult=null;
		 List<RegionInfoPojo> regInfo= getRegionInfo(connection, tableName, cf, qualifier);
		 
		 if(regInfo.size()>0){
			 
			 regInfo= getScanInfo(regInfo, limit, offset, startkey, endkey);
			 
			 if(regInfo != null && regInfo.size()>0){
				  		
				 
				 finalResult= new ArrayList<CellInfo>();
				 
				 for(RegionInfoPojo infoPojo:regInfo){
					 
					try (Table table = connection.getTable(TableName.valueOf(tableName))){
						
					
					 Scan scan = new Scan(Bytes.toBytes(infoPojo.getStartKey()),Bytes.toBytes(infoPojo.getEndKey()));
					 
					  if(qualifier!=null && qualifier.length()>0 
					    		&& cf!=null && cf.length()>0){
							scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
					    }else if(cf!=null && cf.length()>0){
					    	scan.addFamily(Bytes.toBytes(cf));
					    }
					  
					  RowPaginationFilter rowPagFilter=new RowPaginationFilter(infoPojo.getLimit(), infoPojo.getOffset(),infoPojo.getRowsAccepted());
					  
					  if(filter==null){
						 scan.setFilter(rowPagFilter);
					  	  
					  }else{
						  
						 filter.addFilter(rowPagFilter);
						 scan.setFilter(filter);
					  }
					  
					  ResultScanner scanner = table.getScanner(scan);
					  
					  CellInfo info=null;
					  for (Result result : scanner) {
						
						  if(! result.isEmpty()){
							  
							  for(Cell cell:result.listCells()){
								  
								  info= new CellInfo();
								  info.setQualifier(CellUtil.cloneQualifier(cell));
								  info.setRowKey(CellUtil.cloneRow(cell));
								  info.setValue(CellUtil.cloneValue(cell));
								  info.setCf(CellUtil.cloneFamily(cell));
								  
								  finalResult.add(info);
								
							  }
						  }
					  } // scanner
					  
				   }// try
			   } // RegionInfoPojo loop
			 }
		 
		 }else{
			 
			 return null;
		 }
				
		return finalResult;
	}

	
	 /**
	  * Get the Region Info for table
	  * @param connection
	  * @param tableName
	  * @param cf
	  * @param qualifier
	  * @return List<RegionInfoPojo>
	  * @throws Throwable
	  */
	private static List<RegionInfoPojo> getRegionInfo(Connection connection,String tableName,String cf,String qualifier)throws Throwable {
		
         Table table = connection.getTable(TableName.valueOf(tableName));
		 
		 PaginationInput.Builder inputBuilder=PaginationInput.newBuilder();
		 inputBuilder.setColumnFamily(cf);
		 inputBuilder.setQualifier(qualifier);
			
		 final PaginationInput input=inputBuilder.build();
			
		 Map<byte[], PaginationOutput> results = table.coprocessorService(PaginationCoprocessorService.class, null, null,
					new Batch.Call<PaginationCoprocessorService, PaginationOutput>() {
					    
					        public PaginationOutput call(PaginationCoprocessorService aggregate) throws IOException {
					BlockingRpcCallback<PaginationOutput> rpcCallback = new BlockingRpcCallback<PaginationOutput>();
					            aggregate.processCoprocessor(null, input, rpcCallback);
					            PaginationOutput response = rpcCallback.get();
					            return response;
					        }
					    });
		 
		 
		List<RegionInfoPojo> infoList= new ArrayList<RegionInfoPojo>();
		
		 for (Map.Entry<byte[], PaginationOutput> entry : results.entrySet()) {
	            
			PaginationOutput out=entry.getValue();
	        
			RegionInfoPojo info= new RegionInfoPojo();
			
			info.setStartKey(out.getStartKey());
			info.setEndKey(out.getEndKey());
			info.setCount(out.getRegionRecordCount());
			
			infoList.add(info);
	                  
	     }
		
		 table.close();
					 
		return infoList;
	}
	
	
	/**
	 * split the size into different region
	 * @param infoList
	 * @param limit
	 * @param offset
	 * @return List<RegionInfoPojo>
	 */
	private static List<RegionInfoPojo> getScanInfo(List<RegionInfoPojo> infoList,int limit,long offset){
		
		
		List<RegionInfoPojo> outScan= new ArrayList<RegionInfoPojo>();
		
           if(infoList.size()>0){
			 
			 Collections.sort(infoList);
			 
			 RegionInfoPojo infoPojoPrev=null;
			 
			 for(RegionInfoPojo infoPojo:infoList){
						
				if(infoPojoPrev==null){
					infoPojo.setRecordStart(0);
					infoPojo.setRecordEnd(infoPojo.getCount());
				 }else{
							
					infoPojo.setRecordStart(infoPojoPrev.getRecordEnd());
					infoPojo.setRecordEnd(infoPojoPrev.getRecordEnd()+infoPojo.getCount());
				 }
						
				infoPojoPrev=infoPojo;
						
			}
					
			
			RegionInfoPojo lastRegion=infoList.get(infoList.size()-1);
			
			if(lastRegion.getRecordStart()+lastRegion.getCount() < limit+offset ){
				//  limit crossed last region
				//TODO
				return null;
			}
			
			RegionInfoPojo infoPojo=null;
			
			
			for(int index=0; index < infoList.size();index++){
				
				infoPojo=infoList.get(index);
				
				if(infoPojo.getRecordStart() < limit+offset && limit+offset <= infoPojo.getRecordEnd()){
					
					// limit + offset in same region
					
					infoPojo.setLimit(limit);
					infoPojo.setRowsAccepted(infoPojo.getRecordStart());
					infoPojo.setOffset(offset);
					outScan.add(infoPojo);
					
					break;
					
				}else if(infoPojo.getRecordStart() < offset && offset < infoPojo.getRecordEnd() 
						&& limit+offset > infoPojo.getRecordEnd()){
					
					// limit + offset in two regions
					
					if(index+1 < infoList.size()){
						
						RegionInfoPojo infoPojoNext=infoList.get(index+1);
						
						if(infoPojoNext.getRecordStart() < limit+offset && limit+offset <= infoPojoNext.getRecordEnd()){
							
							// limit + offset in same region
							int currentRegionlimit=(int)(infoPojo.getRecordEnd()-offset);
							infoPojo.setLimit(currentRegionlimit);
							infoPojo.setRowsAccepted(infoPojo.getRecordStart());
							infoPojo.setOffset(offset);
							outScan.add(infoPojo);
							
							infoPojoNext.setLimit(limit);
							infoPojoNext.setRowsAccepted(infoPojoNext.getRecordStart());
							infoPojoNext.setOffset(offset);
							outScan.add(infoPojoNext);
							
							break;
							
						}else{
							
							//TODO
							// very big limit and offset, break and throw exception
							outScan=null;
							break;
						}
						
					}else{
						
						// we are at last region , and limit is more
						
						infoPojo.setLimit(limit);
						infoPojo.setRowsAccepted(infoPojo.getRecordStart());
						infoPojo.setOffset(offset);
						outScan.add(infoPojo);
						
						break;
					}
				}
			}
			 
		 }
		
		return outScan;
	}
	
	 
	/**
	 * split the size into different region
	 * @param infoList
	 * @param limit
	 * @param offset
	 * @param scanStartKey
	 * @param scanEndKey
	 * @return List<RegionInfoPojo>
	 */
	private static List<RegionInfoPojo> getScanInfo(List<RegionInfoPojo> infoList,int limit,long offset,
			String scanStartKey,String scanEndKey){
		
		
		List<RegionInfoPojo> outScan= new ArrayList<RegionInfoPojo>();
		
           if(infoList.size()>0){
			
        	Collections.sort(infoList);
			 
			RegionInfoPojo infoPojo=null;
			
			
			for(int index=0; index < infoList.size();index++){
				
				infoPojo=infoList.get(index);
				
				
				if(infoPojo.getStartKey().compareToIgnoreCase(scanStartKey) <= 0 &&
						infoPojo.getEndKey().compareToIgnoreCase(scanEndKey) >= 0 ){
					
					// scan startKey and endkey are in one region
					
					if(limit+offset <= infoPojo.getCount()){
						
						// limit + offset in same region
						
						infoPojo.setStartKey(scanStartKey);
						infoPojo.setEndKey(scanEndKey);
						infoPojo.setLimit(limit);
						infoPojo.setRowsAccepted(infoPojo.getRecordStart());
						infoPojo.setOffset(offset);
						outScan.add(infoPojo);
						
						break;
						
					}else{
						
						//TODO
						// very big limit and offset not in scan range, break and throw exception
						outScan=null;
						break;
					}
					
				} else if(infoPojo.getStartKey().compareToIgnoreCase(scanStartKey) <= 0 
						&& infoPojo.getEndKey().compareToIgnoreCase(scanEndKey) < 0
						&& infoPojo.getEndKey().compareToIgnoreCase(scanStartKey) > 0){
					
                     if(limit+offset <= infoPojo.getCount()){
						
						// limit + offset in same region
						
						infoPojo.setStartKey(scanStartKey);
						infoPojo.setEndKey(scanEndKey);
						infoPojo.setLimit(limit);
						infoPojo.setRowsAccepted(infoPojo.getRecordStart());
						infoPojo.setOffset(offset);
						outScan.add(infoPojo);
						
						break;
						
					}else{
						
						// limit + offset in two regions
						if(index+1 < infoList.size()){
							
							RegionInfoPojo infoPojoNext=infoList.get(index+1);
							
							if(infoPojoNext.getStartKey().compareToIgnoreCase(scanStartKey) > 0 &&
									infoPojoNext.getEndKey().compareToIgnoreCase(scanEndKey) >= 0 ){
								
								
								if(infoPojoNext.getCount()+infoPojo.getCount() >= limit+offset ){
									
									// limit + offset in same region
									int currentRegionlimit=(int)(infoPojo.getCount()-offset);
									
									infoPojo.setStartKey(scanStartKey);
									infoPojo.setRowsAccepted(infoPojo.getRecordStart());
									infoPojo.setOffset(offset);
									infoPojo.setLimit(currentRegionlimit);
									
									outScan.add(infoPojo);
									
									infoPojoNext.setEndKey(scanEndKey);
									infoPojoNext.setLimit(limit);
									infoPojoNext.setRowsAccepted(infoPojo.getCount());
									infoPojoNext.setOffset(offset);
									outScan.add(infoPojoNext);
									break;
									
								}else{
									
									// limit too big, skip scan
									outScan=null;
									break;
								}
								
							}else{
								
								// limit too big, skip scan
								outScan=null;
								break;
							}
							
						}else{
							
							// we are at last region , and limit is more
							
							if(infoPojo.getRecordStart()+infoPojo.getCount() < limit+offset ){
								//  limit crossed last region
								//TODO
								return null;
							}
							infoPojo.setStartKey(scanStartKey);
							infoPojo.setEndKey(scanEndKey);
							infoPojo.setLimit(limit);
							infoPojo.setRowsAccepted(infoPojo.getRecordStart());
							infoPojo.setOffset(offset);
							outScan.add(infoPojo);
							
							break;
						}
					}                
				}
			}
		 }
		
		return outScan;
	}
}
