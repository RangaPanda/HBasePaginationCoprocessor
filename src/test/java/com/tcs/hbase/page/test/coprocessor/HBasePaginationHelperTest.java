package com.tcs.hbase.page.test.coprocessor;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;

import com.tcs.engineering.mediaworks.hbase.pojo.CellInfo;
import com.tcs.engineering.mediaworks.hbase.util.HBasePaginationHelper;
import com.tcs.hbase.page.test.HTablePoolProducer;

public class HBasePaginationHelperTest {



	 

	public static void main(String[] args) {

		 
		 try {

			 String tableName = "TEST_PAGINATION";

			 Connection conn = HTablePoolProducer.getInstance().getHConnection();	

			 System.out.println("Starting " + new Date());
			 
			 
			 //List<CellInfo> result = HBasePaginationHelper.doScan(conn, tableName, "A", "", null,10000, 20);
			 
			 
			 //List<CellInfo> result = HBasePaginationHelper.doScan(conn, tableName, "A", "","4950","5500", null,980, 50);
			 
			 List<CellInfo> result = HBasePaginationHelper.doScan(conn, tableName, "A", "","4950","5000", null,10, 20);
								 
			 HTablePoolProducer.getInstance().cleanUp();
			 System.out.println("end " + new Date());

			 
			 System.out.println("result " + result);
				
		} catch (Throwable ex) {
			ex.printStackTrace();
		}  
	
	}
	
	 


}
