package cmcc.zyhy.hbase.datainsert.migunet.baseuserrecommend;

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
public class ProducerThread5000WAnd12Month extends Thread {
	
	private static final Logger LOG = LoggerFactory.getLogger(ProducerThread5000WAnd12Month.class);
	private HBaseDataManager hbaseDataManager = HBaseDataManager.getHBaseDataManager("migunet_base_user_recommend_table");

	//private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
	//private int addMonth = 0;
	private int x = 0;

	private String[] productSelectors = new String[] { "399643221:特战阅读:3.0", "399643222:特战音乐:3.0", "399643223:特战视频:3.0", "399643224:特战动漫:3.0", "399643225:特战游戏:3.0" };

	public ProducerThread5000WAnd12Month(int x) {
		// this.addMonth = addMonth;
		this.x = x;
		super.setName("thread-" + x);
	}

	@Override
	public void run() {
		hbaseDataManager.setAutoFlush(false);
		hbaseDataManager.setWriteBufferSize(20 * 1024 * 1024);
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.HOUR_OF_DAY, 12);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		long timestamp = cal.getTimeInMillis();
		
		long start = System.currentTimeMillis();

		List<Put> puts = new ArrayList<Put>();
		int loopStart = x * 1000000;
		int loopEnd = (x + 1) * 1000000;
		//int loopStart = x * 10;
		//int loopEnd = (x + 1) * 10;
		int countInsert = 0;
		
		for (int i = loopStart; i < loopEnd; i++) {
			int selectParam = i % 5;
			String phonenum = String.valueOf(1800000000 + i);
			

			String produ = productSelectors[selectParam];
			String rowkey = phonenum;
			Put put = new Put(rowkey.getBytes());
			put.add(BaseUserRecommendTableColumnsUtil.CF_LABEL, BaseUserRecommendTableColumnsUtil.QL_VIDEO_SIM, timestamp, Bytes.toBytes(produ));
			put.add(BaseUserRecommendTableColumnsUtil.CF_LABEL, BaseUserRecommendTableColumnsUtil.QL_MUSIC_SIM, timestamp, Bytes.toBytes(produ));
			put.add(BaseUserRecommendTableColumnsUtil.CF_LABEL, BaseUserRecommendTableColumnsUtil.QL_READ_SIM, timestamp, Bytes.toBytes(produ));
			put.add(BaseUserRecommendTableColumnsUtil.CF_LABEL, BaseUserRecommendTableColumnsUtil.QL_CARTOON_SIM, timestamp, Bytes.toBytes(produ));
			put.add(BaseUserRecommendTableColumnsUtil.CF_LABEL, BaseUserRecommendTableColumnsUtil.QL_GAME_SIM, timestamp, Bytes.toBytes(produ));
			puts.add(put);
			if (puts.size() == 10000) {
				hbaseDataManager.insertDataBatch(puts);
				hbaseDataManager.flushCommits();
				puts.clear();
				countInsert += 10000;
				System.out.println(countInsert+"----------------------");
				LOG.info("[current x: " + x + "];[current phone is " + phonenum + "];" + countInsert+ " records has been insert into migunet_base_user_recommend_table.");
				try {
					Thread.sleep(2000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		/*
		 * hbaseDataManager.insertDataBatch(puts);
		 * hbaseDataManager.flushCommits(); puts.clear();
		 */

		LOG.info("::current x is " + x + ": 1000000 records has been insert into migunet_base_user_recommend_table.");
		LOG.info("::current phone number is [" + String.valueOf(1800000000 + loopEnd -1) + "]");
		// }
		long end = System.currentTimeMillis();
		LOG.info("@@@@@@@@@@@@@producer complete! Total cost[" + (end - start) / 1000 + "]s.");
	}

}
