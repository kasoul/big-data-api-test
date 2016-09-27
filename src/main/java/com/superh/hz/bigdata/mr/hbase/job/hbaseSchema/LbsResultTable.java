package com.superh.hz.bigdata.mr.hbase.job.hbaseSchema;

import org.apache.hadoop.hbase.util.Bytes;

/**
 *  @Describe:lbs结果表——表结构
 */
public interface LbsResultTable {
	public static final String TABLE_NAME = "lbs_result_table";
	
	// lbs_result_table表对应的列族
	public static final byte[] CF_BASICINFO = Bytes.toBytes("basicInfo");
	public static final byte[] CF_TEMPINFO = Bytes.toBytes("tempInfo");

	// lbs_result_table表对应的列限定符
	public static final byte[] QL_WORKLACCELLLIST = Bytes.toBytes("workLacCellList");
	public static final byte[] QL_RESIDENTLACCELLLIST = Bytes.toBytes("residentLacCellList");
	public static final byte[] QL_WORKMODE = Bytes.toBytes("workMode");
	public static final byte[] QL_ONWORKPERIOD = Bytes.toBytes("onWorkPeriod");
	public static final byte[] QL_OFFWORKPERIOD = Bytes.toBytes("offWorkPeriod");

}
