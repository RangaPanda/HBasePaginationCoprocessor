package com.tcs.hbase.page.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


public class HTablePoolProducer {

	 /* A private Constructor prevents any other class from instantiating.  */
	  
	  private HTablePoolProducer(){ }
	  
	  private static HTablePoolProducer singleton= new HTablePoolProducer();
	  
	  
	  /* Static 'instance' method */
	   public static HTablePoolProducer getInstance( ) {
	      return singleton;
	   }
	   
	   
	   public static Connection hConnection =null;
		
	   private static Configuration conf=null;
	   
	   public Connection getHConnection()throws Exception{

	   	  System.out.println(" hTablePool @Produces is calling >>>>>");
	   	    	
	   		//if(hConnection==null){

	   			System.out.println(" hTablePool @Produces first time >>>>>");
				
	   			String host="master.com";
				 
	    		
	    		System.out.println(" host >>>>>"+host+" port 2181 ");
	    			    		
	    		conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", host);
				conf.set("hbase.zookeeper.property.clientPort", "2181");
				
				conf.set("hbase.client.retries.number", "3");
				conf.set("hbase.client.pause", "1000");
				conf.set("zookeeper.recovery.retry", "1");

		    	try {
		    		hConnection = ConnectionFactory.createConnection(conf);
				}catch (Exception e) {
					throw e;
				}
	    	//} 
	   	    
	     return hConnection;
	    
		}
	   
	   public void cleanUp() {

			System.out.println(" @PreDestroy @ is calling >>>>>");

			if (hConnection != null)
				try {
					hConnection.close();
					System.out.println(" @PreDestroy close is done >>>>>");
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
}
