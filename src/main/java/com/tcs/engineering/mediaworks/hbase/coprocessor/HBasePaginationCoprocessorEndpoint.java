package com.tcs.engineering.mediaworks.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationCoprocessorService;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationInput;
import com.tcs.engineering.mediaworks.hbase.proto.generated.coprocessor.PaginationProtoBuffer.PaginationOutput;
/**
 * Coprocessor Endpoint class, can be used to collect the total records based on regions
 * @author Ranga Panda, ranga.panda@gmail.com
 *
 */
public class HBasePaginationCoprocessorEndpoint extends PaginationCoprocessorService implements Coprocessor, CoprocessorService{

	Logger logger = Logger.getLogger(HBasePaginationCoprocessorEndpoint.class);
	
	private RegionCoprocessorEnvironment env;
	
	public Service getService() {
		return this;
	}

	public void start(CoprocessorEnvironment envVal) throws IOException {
		if (envVal instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)envVal;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
		
	}

	public void stop(CoprocessorEnvironment env) throws IOException {
		
	}

	@Override
	public void processCoprocessor(RpcController controller,
			PaginationInput request, RpcCallback<PaginationOutput> done) {
		
		  HRegionInfo hr = env.getRegionInfo();
		  
		 
	      String startKeyStr=Bytes.toString(hr.getStartKey());
          String endKeyStr=Bytes.toString(hr.getEndKey());
	    
          logger.info("HRegion StartKey:" + startKeyStr);
	      logger.info("HRegion EndKey:" + endKeyStr);
	      logger.info("HRegion EndKey:" + endKeyStr);
	      logger.info("getRegionId:" + hr.getRegionId());
	      logger.info("getRegionNameAsString:" + hr.getRegionNameAsString());
          
          PaginationOutput.Builder paginationOutput= PaginationOutput.newBuilder();
          
		  paginationOutput.setEndKey(endKeyStr);
		  paginationOutput.setStartKey(startKeyStr);
		  paginationOutput.setRegionId(hr.getRegionId());
		  paginationOutput.setRegionNameAsString(hr.getRegionNameAsString());
		  
		  long count;
			try {
				count = getRowCount(request);
				paginationOutput.setRegionRecordCount(count);
				logger.info("count:" + count);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e);
			}
		  
		  
		  done.run(paginationOutput.build());
		  
	}
	
	private long getRowCount(PaginationInput request) throws Exception{
		
		    Scan scan = new Scan();
		    long counter = 0;
		     
		    
		    if(request.getQualifier()!=null && request.getQualifier().length()>0 
		    		&& request.getColumnFamily()!=null && request.getColumnFamily().length()>0){
				scan.addColumn(Bytes.toBytes(request.getColumnFamily()), Bytes.toBytes(request.getQualifier()));
		    }else if(request.getColumnFamily()!=null && request.getColumnFamily().length()>0){
		    	scan.addFamily(Bytes.toBytes(request.getColumnFamily()));
		    }
		    scan.setFilter(new FirstKeyOnlyFilter());
		 			
		    RegionScanner scanner = null;
		      			      
		      try {
		    	  
		    	    scanner = env.getRegion().getScanner(scan);
		    	    List<Cell> curVals = new ArrayList<Cell>();
		            boolean doneAllCell = false;
		            do {
		                curVals.clear();
		                doneAllCell = scanner.next(curVals);
		               			               
		                if (curVals.size() > 0) {
		                    counter++;
		                  }
		                
		            } while (doneAllCell);
		            
		       } catch (IOException ioe) {
	            throw ioe;
	           } finally {
	        	   
	        	  try {
	        	        if(scanner!=null)
		                scanner.close();
                     } catch (IOException ignored) {}
		       }
           
		return counter;
	}

}
