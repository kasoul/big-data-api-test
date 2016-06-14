package cmcc.zyhy.hbase.datainsert.miguset.webuserlabel;

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
	private HBaseDataManager hbaseDataManager = HBaseDataManager.getHBaseDataManager("migunet_web_user_label_table");
	
	//private SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
	//private int addMonth = 0;
	private int x = 0;

	private String[] ageSelectors = new String[] { "20", "30", "40", "50", "60" };
	private String[] citySelectors = new String[] { "杭州", "南京", "北京", "上海", "广州" };
	// private String[] internetPrefSelectors = new
	// String[]{"0","1","2","3","4","5","6","7","8","9","10","11"};
	private String[] feeLabelSelectors = new String[] { "10", "20", "30", "40", "50", "60", "70", "80", "90", "100" };
	private String[] activeLabelSelectors = new String[] { "10", "20", "30", "40", "50", "60", "70", "80", "90", "100" };
	private String[] hobbiesSelectors = new String[] { "阅读", "动漫", "音乐", "视频", "游戏" };
	// private String[] styleSelectors = new String[] { "坐车", "度假" };
	private String[] loveStarsSelectors = new String[] { "刘德华", "张学友", "邓超", "范冰冰", "梁静茹" };

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
		long start = System.currentTimeMillis();

		// for (int j = 0; j < 5; j++) {
		List<Put> puts = new ArrayList<Put>();
		int loopStart = x * 1000000;
		int loopEnd = (x + 1) * 1000000;
		int countInsert = 0;
		
		for (int i = loopStart; i < loopEnd; i++) {
			int selectParam = i % 5;
			int random_in_10 = (int) (Math.random() * 10);
			String phonenum = String.valueOf(1800000000 + i);
			long timestamp = cal.getTimeInMillis();

			String gender = i % 2 == 0 ? "男" : "女";
			String ageRange = ageSelectors[selectParam];
			String city = citySelectors[selectParam];
			String internetPref = String.valueOf((int) (Math.random() * 24));
			String feeLabel = feeLabelSelectors[random_in_10];
			String activeLabel = activeLabelSelectors[random_in_10];
			String hobbies = hobbiesSelectors[selectParam];
			String style = "坐车,度假";
			String loveStars = loveStarsSelectors[selectParam];

			String rowkey = phonenum;
			Put put = new Put(rowkey.getBytes());
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_GENDER, timestamp, Bytes.toBytes(gender));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_AGERANGE, timestamp, Bytes.toBytes(ageRange));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_CITY, timestamp, Bytes.toBytes(city));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_INTERNETPREF, timestamp, Bytes.toBytes(internetPref));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_FEE_LABEL, timestamp, Bytes.toBytes(feeLabel));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_ACTIVE_LABEL, timestamp, Bytes.toBytes(activeLabel));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_HOBBIES, timestamp, Bytes.toBytes(hobbies));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_STYLE, timestamp, Bytes.toBytes(style));
			put.add(WebUserLabelTableColumnsUtil.CF_LABEL, WebUserLabelTableColumnsUtil.QL_LOVESTARS, timestamp, Bytes.toBytes(loveStars));

			puts.add(put);
			if (puts.size() == 10000) {
				hbaseDataManager.insertDataBatch(puts);
				hbaseDataManager.flushCommits();
				puts.clear();
				countInsert += 10000;
				LOG.info("[current x: " + x + "];[current phone is " + phonenum + "];" + countInsert+ " records has been insert into migunet_web_user_label_table.");
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

		LOG.info("::current x is " + x + ": 1000000 records has been insert into migunet_web_user_label_table.");
		LOG.info("::current phone number is [" + String.valueOf(1800000000 + loopEnd -1) + "]");
		// }
		long end = System.currentTimeMillis();
		LOG.info("@@@@@@@@@@@@@producer complete! Total cost[" + (end - start) / 1000 + "]s.");
	}

}
