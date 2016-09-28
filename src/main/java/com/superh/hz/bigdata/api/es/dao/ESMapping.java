package com.superh.hz.bigdata.api.es.dao;


/**
 * elastic mapping
 */
public class ESMapping {
	
	public final static String TEST_MAPPING= ""
			+ "{"
			+ "	/*索引类型名*/"
			+ "	\"data\": {"
			+ "		\"properties\": {"
			+ "			/*string 类型属性*/"
			+ "			\"property1\": {\"type\": \"string\", \"index\": \"no\"},"
			+ "			/*byte 类型属性*/"
			+ "			\"property2\": {\"type\": \"byte\"},"
			+ "			/*int 类型属性*/"
			+ "			\"property3\": {\"type\": \"int\"},"
			+ "		}"
			+ "	}"
			+ "}";
	
}
