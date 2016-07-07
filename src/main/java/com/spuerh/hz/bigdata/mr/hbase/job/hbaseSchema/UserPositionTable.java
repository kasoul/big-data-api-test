package com.spuerh.hz.bigdata.mr.hbase.job.hbaseSchema;

import org.apache.hadoop.hbase.util.Bytes;

/**
 *  @Describe:用户位置表——表结构
 */
public interface UserPositionTable {
	public static final String TABLE_NAME = "user_position_table";
	
	// user_position_table表对应的列族
	public static final byte[] CF_POSITION = Bytes.toBytes("position");

	// user_position_table表对应的列限定符
	public static final byte[] QL_TIMESTAMP = Bytes.toBytes("timestamp");
	public static final byte[] QL_DURATION = Bytes.toBytes("duration");
	public static final byte[] QL_LACCELL = Bytes.toBytes("lacCell");
	
}
