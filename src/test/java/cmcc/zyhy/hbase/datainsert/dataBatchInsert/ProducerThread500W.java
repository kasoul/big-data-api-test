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
* @File: ProducerThread500W.java 
* @Date: 2014年11月28日 
* @Author: 黄超（huangchaohz）
* @Copyright: 版权所有 (C) 2014 中国移动 杭州研发中心. 
*
* @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
*/
public class ProducerThread500W extends Thread {
	//private static final Log LOG = LogFactory.getLog(ProducerThreadA.class);
	private static final Logger LOG = LoggerFactory.getLogger(ProducerThread500W.class);
	private HBaseDataManager hbaseDataManager = HBaseDataManager.getHBaseDataManager("user_position_table");
	private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
	private Calendar cal = null;
	private int x = 0;
	String random_4len_string = String.format("%04d", (int) (Math.random() * 9999));
	String random_3len_string = String.format("%03d", (int) (1000 + Math.random() * 999));
	//String random_1len_string = String.format("%01d", (int) (Math.random() * 2));
	String imsi = "4600271781aaaaattttt";
	String lac = random_4len_string;
	String cellid = "5" + random_4len_string;
	String lacCell = "18db-" + random_3len_string;
	
	public ProducerThread500W(Calendar cal,int x){
		this.cal = cal;
		this.x = x;
		super.setName("thread-"+x);
	}
	
	@Override
	public void run() {
		hbaseDataManager.setAutoFlush(false);
		hbaseDataManager.setWriteBufferSize(20 * 1024 * 1024);
		//Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		long start = System.currentTimeMillis();
		for (int j = 0; j < 24; j++) {

			List<Put> puts = new ArrayList<Put>();
			//for (int x = 0; x < 500; x++) {
				//long start = System.currentTimeMillis();
					Integer call = 65000000 + x * 100000;
					for (int i = 0; i < 100000; i++) {
					// long start_loop = System.currentTimeMillis();
						//Random rand = new Random();
						
						String phonenum = "188" + String.valueOf(call);
						String timestamp = format.format(cal.getTime());
						//String flag = random_1len_string;
						//String eventid = "05.08.1";
						//String cause = "0";
						//String res = "0";
						//String opp_number = "1886711" + random_4len_string;
						//double lon = 120.16809977789;
						double lat = 30.278121247584 + Math.random()*0.1+x*0.001;
						String lacCoordinate = "120.16809977789," + String.valueOf(lat);
						
						String rowkey = phonenum + "-" + timestamp;
						
						Put put = new Put(rowkey.getBytes());
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_IMSI, Bytes.toBytes(imsi));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_MSISDN, Bytes.toBytes(phonenum));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_TIMESTAMP, Bytes.toBytes(timestamp));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_LAC, Bytes.toBytes(lac));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_CELLID, Bytes.toBytes(cellid));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_EVENTID, Bytes.toBytes("0000"));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_CAUSE, Bytes.toBytes("0"));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_FLAG, Bytes.toBytes("0"));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_RES, Bytes.toBytes("0"));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_OPPNUMBER, Bytes.toBytes("0"));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_LACCELL, Bytes.toBytes(lacCell));
						put.add(UserPositionTableColumnsUtil.CF_POSITION, UserPositionTableColumnsUtil.QL_LACCOORDINATE, Bytes.toBytes(lacCoordinate));
						
						puts.add(put);
						if(puts.size()==10000){
							hbaseDataManager.insertDataBatch(puts);
							hbaseDataManager.flushCommits();
							puts.clear();
							LOG.info(format.format(cal.getTime()) + "]: "
							 + "phone prefix is " + x + ": " + (j * 100000 +i+1) + " records has been insert into user_position_table.");
							try {
								Thread.sleep((x % 10) * 1000L);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						call = call + 1;
					}
					/*hbaseDataManager.insertDataBatch(puts);
					hbaseDataManager.flushCommits();
					puts.clear();*/
					
					LOG.info("::phone prefix is " + x + ": " + (j+1) * 100000 + " records has been insert into user_position_table.");
					LOG.info("::current time is [" + format.format(cal.getTime()) + "]");
					LOG.info("::current phone number is [" + "188" + String.valueOf(call-1) + "]");
					
			//}
			cal.add(Calendar.HOUR_OF_DAY, 1);
		}
		long end = System.currentTimeMillis();
		LOG.info("@@@@@@@@@@@@@producer complete! Total cost[" + (end - start)/1000 + "]s.");
		
		//hbaseDataManager.f
	}

}
