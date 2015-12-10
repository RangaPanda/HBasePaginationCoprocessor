package com.tcs.hbase.page.test.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import com.tcs.engineering.mediaworks.hbase.pojo.RegionInfoPojo;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationCoprocessorService;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationInput;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationOutput;
import com.tcs.hbase.page.test.HTablePoolProducer;

public class HBasePaginationCoprocessorEndpointTest {

 

	public static void main(String[] args) {

		 
		 try {

			 String tableName = "TEST_PAGINATION";

			 Connection conn = HTablePoolProducer.getInstance().getHConnection();	

			 System.out.println("Starting " + new Date());
			 
			 Table table = conn.getTable(TableName.valueOf(tableName));
			 
			 PaginationInput.Builder inputBuilder=PaginationInput.newBuilder();
			 inputBuilder.setColumnFamily("A");
				
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
		            
		          //  System.out.println("Region: " + Bytes.toString(entry.getKey()));
		            
				PaginationOutput out=entry.getValue();
		        
				RegionInfoPojo info= new RegionInfoPojo();
				
				info.setStartKey(out.getStartKey());
				info.setEndKey(out.getEndKey());
				info.setCount(out.getRegionRecordCount());
				
				infoList.add(info);
		                  
		     }
			
		 
			  HTablePoolProducer.getInstance().cleanUp();
			  System.out.println("end " + new Date());

			  Collections.sort(infoList);
				
			 System.out.println(infoList);
				
		} catch (Throwable ex) {
			ex.printStackTrace();
		}  
	
	}
	
	 

}
