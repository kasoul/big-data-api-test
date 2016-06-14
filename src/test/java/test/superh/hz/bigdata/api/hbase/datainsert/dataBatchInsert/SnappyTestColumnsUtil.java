package test.superh.hz.bigdata.api.hbase.datainsert.dataBatchInsert;

import org.apache.hadoop.hbase.util.Bytes;

public class SnappyTestColumnsUtil {
	// user_position_table表对应的列族
	public static final byte[] CF_POSITION = Bytes.toBytes("colfam");

	
	// user_position_table表对应的列限定符
	public static final byte[] QL_IMSI = Bytes.toBytes("imsi");
	public static final byte[] QL_MSISDN = Bytes.toBytes("msisdn");
	public static final byte[] QL_TIMESTAMP = Bytes.toBytes("timestamp");
	public static final byte[] QL_LAC = Bytes.toBytes("lac");
	public static final byte[] QL_CELLID = Bytes.toBytes("cellId");
	public static final byte[] QL_EVENTID = Bytes.toBytes("eventId");
	public static final byte[] QL_CAUSE = Bytes.toBytes("cause");
	public static final byte[] QL_FLAG = Bytes.toBytes("flag");
	public static final byte[] QL_RES = Bytes.toBytes("res");
	public static final byte[] QL_OPPNUMBER = Bytes.toBytes("oppNumber");
	public static final byte[] QL_LACCELL = Bytes.toBytes("lacCell");
	public static final byte[] QL_LACCOORDINATE = Bytes.toBytes("lacCoordinate");
}
