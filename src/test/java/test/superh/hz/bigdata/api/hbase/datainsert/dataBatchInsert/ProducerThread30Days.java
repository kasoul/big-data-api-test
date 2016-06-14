package test.superh.hz.bigdata.api.hbase.datainsert.dataBatchInsert;

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
 *  2014-11-28
 */
public class ProducerThread30Days extends Thread {
	//private static final Log LOG = LogFactory.getLog(ProducerThreadA.class);
	private static final Logger LOG = LoggerFactory.getLogger(ProducerThread30Days.class);
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
	
	public ProducerThread30Days(Calendar cal){
		this.cal = cal;
		this.cal.set(Calendar.HOUR_OF_DAY, 0);
		this.cal.set(Calendar.MINUTE, 0);
		this.cal.set(Calendar.SECOND, 0);
		super.setName("thread-"+format.format(cal.getTime()));
		//System.out.println(this.getName()+":"+ format.format(this.cal.getTime()));
	}
	
	@Override
	public void run() {
		hbaseDataManager.setAutoFlush(false);
		hbaseDataManager.setWriteBufferSize(20 * 1024 * 1024);
		long start = System.currentTimeMillis();
		List<Put> puts = new ArrayList<Put>();
		//System.out.println(this.getName()+":::"+ format.format(this.cal.getTime()));
		for (int j = 0; j < 1440; j++) {
					Integer call = 64000000;
					
					for (int i = 0; i < 500; i++) {	
						String phonenum = "188" + String.valueOf(call);
						String timestamp = format.format(cal.getTime());
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
						call = call + 1;
					}
					if(puts.size() >= 10000){
						hbaseDataManager.insertDataBatch(puts);
						hbaseDataManager.flushCommits();
						LOG.info("[" + format.format(cal.getTime()) + "]: " + 
								 puts.size() + " records has been insert into user_position_table.");
						puts.clear();
						try {
							Thread.sleep(3000L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
			cal.add(Calendar.MINUTE,1);
		}
		cal.add(Calendar.MINUTE,-1);
		hbaseDataManager.insertDataBatch(puts);
		hbaseDataManager.flushCommits();
		LOG.info("[" + format.format(cal.getTime()) + "]: " + 
				 puts.size() + " records has been insert into user_position_table.");
		puts.clear();
		
		LOG.info("::current time is [" + format.format(cal.getTime()) + "]");
		LOG.info("::current records inserted is [" + 1440*500 + "]");
		long end = System.currentTimeMillis();
		LOG.info("@@@@@@@@@@@@@producer complete! Total cost[" + (end - start)/1000 + "]s.");
		
		//hbaseDataManager.f
	}

}
