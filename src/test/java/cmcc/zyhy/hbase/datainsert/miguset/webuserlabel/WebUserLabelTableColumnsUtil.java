package cmcc.zyhy.hbase.datainsert.miguset.webuserlabel;

import org.apache.hadoop.hbase.util.Bytes;

public class WebUserLabelTableColumnsUtil {
	// user_position_table表对应的列族
	public static final byte[] CF_LABEL = Bytes.toBytes("label");

	
	// user_position_table表对应的列限定符
	public static final byte[] QL_GENDER = Bytes.toBytes("gender");
	public static final byte[] QL_AGERANGE = Bytes.toBytes("ageRange");
	public static final byte[] QL_CITY = Bytes.toBytes("city");
	public static final byte[] QL_INTERNETPREF = Bytes.toBytes("internetpref");
	public static final byte[] QL_FEE_LABEL = Bytes.toBytes("fee_label");
	public static final byte[] QL_ACTIVE_LABEL = Bytes.toBytes("active_label");
	public static final byte[] QL_HOBBIES = Bytes.toBytes("hobbies");
	public static final byte[] QL_STYLE = Bytes.toBytes("style");
	public static final byte[] QL_LOVESTARS = Bytes.toBytes("loveStars");
}
