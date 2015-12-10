# HBasePaginationCoprocessor

# HBase Row Pagination:  it will address the HBASE-3453 jira, and handle the records split in region, it will handled pagination from particular position.

# Steps: 
1.	Get  row count of all regions  using endpoint Coprocessor
2.	Sort the region based on start key, so that we know each region how many rows holding, and its easy to jump for particular region for scan.
3.	Loop through the sorted list and find where use user provided Offset and limit fall in.
4.	If offset and limit fall in two regions, then scan in two regions, find how many records from first region and how many records from second region.
5.	If offset and limit in one region , execute scan with star and end keys

# Work done:
HBasePaginationCoprocessorEndpoint.java : its endpoint Coprocessor , run all region parallel and count rows in each region.
RowPaginationFilter.java:  it’s a filter class, will include only rows which fall in offset and limit.
HBasePaginationHelper.java : its main helper class, user will use to call pagination,

# Example:

Connection conn = HTablePoolProducer.getInstance().getHConnection();	

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
List<CellInfo> result = HBasePaginationHelper.doScan(conn, “TEST_PAGINATION”, "A", "", null,10000, 20);

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
List<CellInfo> result = HBasePaginationHelper.doScan(conn, “TEST_PAGINATION”, "A", "","4950","5000", null,10, 20);
			 
HTablePoolProducer.getInstance().cleanUp();

# Installing Coprocessor
put the HBasePaginationCoprocessor-1.0.0.jar in all region server and register
<property>  
 <name>hbase.coprocessor.region.classes</name>    <value>com.tcs.engineering.mediaworks.hbase.coprocessor.HBasePaginationCoprocessorEndpoint</value>
 </property>
 
# GIRA
https://issues.apache.org/jira/browse/HBASE-3453

