package cmcc.zyhy.hbase.datainsert.dataBatchInsert;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.superh.hz.bigdata.api.hbase.dao.HBaseDataManager;

/**
 * 
 * @Project: hbase-accessor
 * @File: ProducerThread50WAnd30Days.java
 * @Date: 2014年11月28日
 * @Author: 黄超（huangchaohz）
 * @Copyright: 版权所有 (C) 2014 中国移动 杭州研发中心.
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的
 */
public class ProducerThreadSnappyTest extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerThreadSnappyTest.class);
	private HBaseDataManager hbaseDataManager = HBaseDataManager.getHBaseDataManager("snappy_test");
	private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
	private Calendar cal = null;
	private int x = 0;
	String random_4len_string = String.format("%04d", (int) (Math.random() * 9999));
	String random_3len_string = String.format("%03d", (int) (1000 + Math.random() * 999));
	// String random_1len_string = String.format("%01d", (int) (Math.random() *
	// 2));
	String imsi = "4600271781aaaaattttt";
	String lac = random_4len_string;
	String cellid = "5" + random_4len_string;
	String lacCell = "18db-" + random_3len_string;
	
	public ProducerThreadSnappyTest(Calendar cal,int x){
		this.cal = cal;
		this.x = x;
		super.setName("thread-"+x);
	}

	@Override
	public void run() {
		hbaseDataManager.setAutoFlush(false);
		hbaseDataManager.setWriteBufferSize(20 * 1024 * 1024);
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		long start = System.currentTimeMillis();
		List<Put> puts = new ArrayList<Put>();
		for (int j = 0; j < 36 ; j++) {
			Integer call = 66500000 + x * 10000;
			for (int i = 0; i < 10000; i++) {
				String phonenum = "188" + String.valueOf(call);
				String timestamp = format.format(cal.getTime());
				double lat = 30.278121247584 + Math.random() * 0.1 + x * 0.001;
				String lacCoordinate = "120.16809977789," + String.valueOf(lat);

				String rowkey = phonenum + "-" + timestamp;

				Put put = new Put(rowkey.getBytes());
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_IMSI, Bytes.toBytes(imsi));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_MSISDN, Bytes.toBytes(phonenum));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_TIMESTAMP, Bytes.toBytes(timestamp));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_LAC, Bytes.toBytes(lac));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_CELLID, Bytes.toBytes(cellid));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_EVENTID, Bytes.toBytes("0000"));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_CAUSE, Bytes.toBytes("0"));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_FLAG, Bytes.toBytes("0"));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_RES, Bytes.toBytes("0"));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_OPPNUMBER, Bytes.toBytes("0"));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_LACCELL, Bytes.toBytes(lacCell));
				put.add(SnappyTestColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_LACCOORDINATE, Bytes.toBytes(lacCoordinate));

				puts.add(put);
				call = call + 1;
			}
			hbaseDataManager.insertDataBatch(puts);
			hbaseDataManager.flushCommits();
			LOG.info("[" + format.format(cal.getTime()) + "]: " + "phone-prefix is " + (1886650+x) + ": " 
			+ ((j+1) * 10000) + " records has been insert into user_position_table.");
			puts.clear();
			try {
				Thread.sleep((x % 10) * 1000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cal.add(Calendar.MINUTE, 20);
		}
		long end = System.currentTimeMillis();
		LOG.info("::current day-time is [" + format.format(cal.getTime()) + "]");
		LOG.info("::current phone-frefix "+ (1886650+x) + ":records inserted is [" + (36*1) * 10000 + "]");
		LOG.info("@@@@@@@@@@@@@producer complete! Total cost[" + (end - start) / 1000 + "]s.");
	}

}
