package com.tcs.hbase.page.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class ReginInfo {

	public static byte[] bHashA = Bytes.toBytes("A");
	public static  byte[] qual = Bytes.toBytes("D");


	public static void main(String[] args) {

		 
		 try {

			 String tableName = "TEST_PAGINATION";

			 Connection conn = HTablePoolProducer.getInstance().getHConnection();	

			 System.out.println("Starting " + new Date());
			 
			 List<byte[]> out=getRegionOfTable(conn,tableName);
			 
			  getOneRecord(conn, tableName, out);
			  HTablePoolProducer.getInstance().cleanUp();
			  System.out.println("end " + new Date());

		} catch (Throwable ex) {
			ex.printStackTrace();
		}  
	
	}
	
	
	public static List<byte[]>  getRegionOfTable(Connection conn,String tabName){
	    org.apache.hadoop.hbase.TableName tn = org.apache.hadoop.hbase.TableName.valueOf(tabName);
	    List<byte[]> out= new ArrayList<byte[]>();
	    HRegionInfo ob;
	    try{
	    	 
	        Admin hba = conn.getAdmin();
	        List<HRegionInfo> lr = hba.getTableRegions(tn);
	        Iterator<HRegionInfo> ir = lr.iterator();
	        while(ir.hasNext()){
	            ob = ir.next();
	            System.out.println("RegionNameAsString "+ob.getRegionNameAsString());
	            System.out.println(" RegionId "+ob.getRegionId());
	            System.out.println("RegionName "+Bytes.toString(ob.getRegionName()));
	            System.out.println("start "+Bytes.toString(ob.getStartKey()));
	            System.out.println("End "+Bytes.toString(ob.getEndKey()));
	            
	            out.add(ob.getEndKey());
	            out.add(ob.getStartKey());
	        }
	        hba.close();
	        
	        
	    }catch(Exception ex){
	        ex.printStackTrace();
	    }
	    
	    return out;
	}
	 
	

	 /**
	     * Get a row
	     */
	    public static void getOneRecord (Connection conn,String tableName, List<byte[]> out) throws IOException{
	    	Table table = conn.getTable(TableName.valueOf(tableName));
	    	
	    	for(byte[] row:out){
	    		
	    		if(row.length>0){
	    		
	    		Get get = new Get(row);
	    		get.addFamily(bHashA);
	    		
		        Result rs = table.get(get);
		        if(!rs.isEmpty()){
		        	
		        	System.out.println(" rs not empty ");
		        	
		        	for(Cell kv : rs.listCells()){
			            System.out.print(Bytes.toString(CellUtil.cloneRow(kv)));
			            System.out.print(Bytes.toString(CellUtil.cloneQualifier(kv)));
			            System.out.print(kv.getTimestamp() + " " );
			        }
		        }
	    		}
		        
	    	}
	        
	        table.close();
	    }
	 
}
