package com.tcs.hbase.page.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



public class WriteSigData {

	public static void main(String[] args) {

		 //create 'TEST_PAGINATION', 'A', {SPLITS => ['1000','2000','3000','4000','5000','6000','7000','8000','9000']}
		 try {

			 String tableName = "TEST_PAGINATION";

			

			 System.out.println("Starting " + new Date());
			 
			 
		        for (int i=1000; i<12000; i+=1000){
		        	
		                System.out.println(i);
		               
		                List<Put> batch= createHashedPasPutList(i);
		                
		                try {
		                	
		                	Connection conn = HTablePoolProducer.getInstance().getHConnection();	
		                	Table table = conn.getTable(TableName.valueOf(tableName));
		                	table.put(batch);
		                	table.close();
		                } catch (Throwable ex) {
		                	ex.printStackTrace();
		        		}finally {
		        			try {
		        				HTablePoolProducer.getInstance().cleanUp();

		        			} catch (Exception exp) {
		        				exp.printStackTrace();
		        			}
		        		} 
		        }
			
			 
			  System.out.println("end " + new Date());
			 

		} catch (Throwable ex) {
			ex.printStackTrace();
		}  
	
	}
	
	
	/**
	 * Create a List of row/PUT objects from SignatureData
	 * @param sigData
	 * @return List<Put>
	 * @throws Exception
	 */
	public static List<Put> createHashedPasPutList(int start) throws Exception {
		List<Put> batch = new ArrayList<Put>();

		byte[] bHashA = Bytes.toBytes("A");
		byte[] qual = Bytes.toBytes("D");
				
		System.out.println("start size >>>>"+start);
        String key="";
		for (int i=0;i <1000; i++) {
			key=(i+start)+"";
			Put put = new Put(key.getBytes());
			put.addColumn(bHashA, qual, key.getBytes());
			batch.add(put);
		}

		return batch;
	}
	
	
	

}
