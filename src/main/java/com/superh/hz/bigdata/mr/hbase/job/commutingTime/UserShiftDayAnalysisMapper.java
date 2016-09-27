package com.superh.hz.bigdata.mr.hbase.job.commutingTime;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.superh.hz.bigdata.mr.hbase.job.hbaseSchema.UserPositionTable;
import com.superh.hz.bigdata.mr.util.hadoop.JobConstants;


public class UserShiftDayAnalysisMapper extends TableMapper<Text, Text> {
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	
	private String currentDate = null;
	private String beginDate = null;
	
	private static Logger log = null;

	@Override
	protected void setup(Context context) {
		
		currentDate = context.getConfiguration().get(JobConstants.CURRENT_DATE_PROPERTY_NAME);
		beginDate = context.getConfiguration().get(JobConstants.BEGIN_DATE_PROPERTY_NAME);
		
		log = LoggerFactory.getLogger(UserShiftDayAnalysisMapper.class);
		// map开始
		log.info("user shift day analysis mapper begin");
	}

	@Override
	protected void map(ImmutableBytesWritable rowKey, Result columns,
			Context context) throws IOException, InterruptedException {

		
		String key = Bytes.toString(rowKey.get());
		String[] keySplit = key.split("-");
		if (keySplit.length != 2) {
			return;
		}
		
		String phoneNumber = keySplit[0];
		String timeStamp = keySplit[1];
		 if(timeStamp.length()!=14){
			 return;
		 }
		String yyyyMMdd = timeStamp.substring(0, 8);
		String hhmmss = timeStamp.substring(8, 14);
		
		if(currentDate == null || beginDate == null){
			return;
		}else if (yyyyMMdd.compareTo(beginDate)<0 || yyyyMMdd.compareTo(currentDate)>0) {
			return;
		}
		
		try {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(simpleDateFormat.parse(yyyyMMdd));
			if(isWeekend(calendar)){
				return;
			}
		} catch (ParseException e1) {
			log.error("日期格式错误："+yyyyMMdd);
			return;
		}
		keyOut.set(phoneNumber + "-" + yyyyMMdd);
		if (columns.containsColumn(UserPositionTable.CF_POSITION,UserPositionTable.QL_LACCELL)
//				&& columns.containsColumn(UserPositionTable.CF_POSITION,UserPositionTable.QL_DURATION)
				) {
//			String duration = Bytes.toString(columns.getValue(
//					UserPositionTable.CF_POSITION,
//					UserPositionTable.QL_DURATION));
			String laccell = Bytes.toString(columns
					.getValue(UserPositionTable.CF_POSITION,
							UserPositionTable.QL_LACCELL));
//			if (duration.equals("") || !duration.matches("[0-9]+")) {
//				log.error(phoneNumber + " duratioon value illegal");
//				return;
//			}
			valueOut.set(hhmmss + "-" + laccell );//+ "-" + duration
			try {
				context.write(keyOut, valueOut);
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
	}

	protected boolean isWeekend(Calendar cal) {
		int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
		if (week == 6 || week == 0) {// 0代表周日，6代表周六
			return true;
		}
		return false;
	}
}
