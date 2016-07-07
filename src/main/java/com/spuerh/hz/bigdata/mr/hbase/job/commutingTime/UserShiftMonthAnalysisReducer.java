package com.spuerh.hz.bigdata.mr.hbase.job.commutingTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spuerh.hz.bigdata.mr.hbase.job.hbaseSchema.LbsResultTable;
import com.spuerh.hz.bigdata.mr.util.common.EncryptUtil;

public class UserShiftMonthAnalysisReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{

	private static Logger log = null;
	
	@Override
	protected void setup(Context context){
		log = LoggerFactory.getLogger(UserShiftMonthAnalysisReducer.class);
		log.info("user shift month analysis reducer begin");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		List<String> dayShiftList = new ArrayList<String>();
		List<String> nightShiftList = new ArrayList<String>();
		for (Text val : values) {
			if(val.toString().indexOf("0-")!=-1){
				dayShiftList.add(val.toString());
			}else if(val.toString().indexOf("1-")!=-1){
				nightShiftList.add(val.toString());
			}
		}
		int th1=0;
		int th2=0;
		int tw1=0;
		int tw2=0;
		int th3=0;
		int tw3=0;
		if(dayShiftList.size() >= nightShiftList.size() ){//白班
			for (String str : dayShiftList) {
				String[] periods = str.split("-");
				th2+=timeToSecond(periods[1]);
				tw1+=timeToSecond(periods[2]);
				tw2+=timeToSecond(periods[3]);
				th3+=timeToSecond(periods[4]);
			}
			String rowKey = EncryptUtil.encrypt(key.toString(), "MD5");
			Put put = new Put(rowKey.getBytes());
			put.addColumn(LbsResultTable.CF_TEMPINFO, LbsResultTable.QL_WORKMODE, "0".getBytes());
			put.addColumn(LbsResultTable.CF_TEMPINFO, LbsResultTable.QL_ONWORKPERIOD, (secondToTime(th2/dayShiftList.size())+"-"+secondToTime(tw1/dayShiftList.size())).getBytes());
			put.addColumn(LbsResultTable.CF_TEMPINFO, LbsResultTable.QL_OFFWORKPERIOD, (secondToTime(tw2/dayShiftList.size())+"-"+secondToTime(th3/dayShiftList.size())).getBytes());
			try 
			{
				context.write(new ImmutableBytesWritable(rowKey.getBytes()), put);
			} 
			catch (Exception e) 
			{
				log.error(e.getMessage(), e);
			}
		}else if(dayShiftList.size() < nightShiftList.size()){//夜班
			for (String str : nightShiftList) {
				String[] periods = str.split("-");
				tw2+=timeToSecond(periods[1]);
				th1+=timeToSecond(periods[2]);
				th2+=timeToSecond(periods[3]);
				tw3+=timeToSecond(periods[4]);
			}
			String rowKey = EncryptUtil.encrypt(key.toString(), "MD5");
			Put put = new Put(rowKey.getBytes());
			put.addColumn(LbsResultTable.CF_TEMPINFO, LbsResultTable.QL_WORKMODE, "1".getBytes());
			put.addColumn(LbsResultTable.CF_TEMPINFO, LbsResultTable.QL_ONWORKPERIOD, (secondToTime(th2/nightShiftList.size())+"-"+secondToTime(tw3/nightShiftList.size())).getBytes());
			put.addColumn(LbsResultTable.CF_TEMPINFO, LbsResultTable.QL_OFFWORKPERIOD, (secondToTime(tw2/nightShiftList.size())+"-"+secondToTime(th1/nightShiftList.size())).getBytes());
			try 
			{
				context.write(new ImmutableBytesWritable(rowKey.getBytes()), put);
			} 
			catch (Exception e) 
			{
				log.error(e.getMessage(), e);
			}
		}
		
	}
	
	/**
	 * 时间转化为秒数
	 * @param String: time 时间，hhmiss
	 * @return Integer: 当天距离00:00:00的秒数
	 */
	protected Integer timeToSecond(String time){
		int hours = Integer.parseInt(time.substring(0,2));
		int minutes = Integer.parseInt(time.substring(2, 4));
		int seconds = Integer.parseInt(time.substring(4,6));
		return hours*3600 + minutes*60 + seconds;
	}
	
	/**
	 * 秒数转化为时间
	 * @param Integer: second 当天距离00:00:00的秒数
	 * @return String ：时间，hhmiss
	 */
	protected String secondToTime(Integer second){
		int hours = second/3600;
		int remainder = second%3600;
		int minutes = remainder/60;
		int seconds= remainder%60;
		return hours + ":" + minutes + ":" + seconds;
	}
	
}
