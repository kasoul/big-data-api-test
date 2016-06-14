package cmcc.zyhy.hbase.datainsert.migunet.baseuserrecommend;

import org.apache.hadoop.hbase.util.Bytes;

public class BaseUserRecommendTableColumnsUtil {
	// user_position_table表对应的列族
	public static final byte[] CF_LABEL = Bytes.toBytes("recommend");

	
	// user_position_table表对应的列限定符
	public static final byte[] QL_VIDEO_SIM = Bytes.toBytes("videoSimilarity");
	public static final byte[] QL_MUSIC_SIM = Bytes.toBytes("musicSimilarity");
	public static final byte[] QL_READ_SIM = Bytes.toBytes("readSimilarity");
	public static final byte[] QL_CARTOON_SIM = Bytes.toBytes("cartoonSimilarity");
	public static final byte[] QL_GAME_SIM = Bytes.toBytes("gameSimilarity");
}
