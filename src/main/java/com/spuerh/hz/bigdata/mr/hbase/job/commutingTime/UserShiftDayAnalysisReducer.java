package com.spuerh.hz.bigdata.mr.hbase.job.commutingTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spuerh.hz.bigdata.mr.hbase.job.hbaseSchema.LbsResultTable;
import com.spuerh.hz.bigdata.mr.util.common.EncryptUtil;

public class UserShiftDayAnalysisReducer extends
		Reducer<Text, Text, Text, Text> {
	private static Logger log = LoggerFactory.getLogger(UserShiftDayAnalysisReducer.class);
	private String residentPlace = "";// 居住地
	private String workPlace = "";// 工作地
	private Table hTable = null;

	private Map<String,String> userResidentAdderessMap = new HashMap<String,String>();
	private Map<String,String> userWorkAdderessMap = new HashMap<String,String>();
	private int countR = 0;
	private int countW = 0;

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			hTable = connection.getTable(TableName.valueOf(LbsResultTable.TABLE_NAME));
		} catch (IOException e) {
			log.error("htable connect error.",e);
		}
		
		log.info("user shift day analysis reducer begin");
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String ROW_KEY = key.toString().split("-")[0];
		ROW_KEY = EncryptUtil.encrypt(ROW_KEY, "MD5");
		if(userResidentAdderessMap.containsKey(ROW_KEY) && userWorkAdderessMap.containsKey(ROW_KEY)){
			residentPlace = userResidentAdderessMap.get(ROW_KEY);
			workPlace = userWorkAdderessMap.get(ROW_KEY);
		}else{
			Get get = new Get(ROW_KEY.getBytes());
			Result result = hTable.get(get);
			String[] residentLacCellList = null;
			String[] workLacCellList = null;
			byte[] residentLacCellListStr = result.getValue(LbsResultTable.CF_TEMPINFO,LbsResultTable.QL_RESIDENTLACCELLLIST);
			if(residentLacCellListStr != null && !residentLacCellListStr.equals("")){
				residentLacCellList = new String(residentLacCellListStr).split(";");//分割分开，获取最后一个
				String residentPlaceNew = residentLacCellList[residentLacCellList.length-1].split(":")[1];
				userResidentAdderessMap.put(ROW_KEY, residentPlaceNew);
				residentPlace = residentPlaceNew;
			}else{
				log.error("未查询到用户：" + ROW_KEY + "的居住地信息！");
				return;
			}
			byte[] workLacCellListStr = result.getValue(LbsResultTable.CF_TEMPINFO,LbsResultTable.QL_WORKLACCELLLIST);
			if(workLacCellListStr != null && !workLacCellListStr.equals("")){
				workLacCellList = new String(workLacCellListStr).split(";");//分割分开，获取最后一个
				String workPlaceNew = workLacCellList[workLacCellList.length-1].split(":")[1];
				userWorkAdderessMap.put(ROW_KEY, workPlaceNew);
				workPlace = workPlaceNew;
			}else{
				log.error("未查询到用户：" + ROW_KEY + "的工作地信息！");
				return;
			}
		}
		
		List<String> workDatas = new ArrayList<String>();
		List<String> residentDatas = new ArrayList<String>();
		for (Text val : values) {// hhmmss + "-" + lacCell 
			if (workPlace.equals(val.toString().split("-")[1])) {
				workDatas.add(val.toString().split("-")[0]);
			} else if (residentPlace.equals(val.toString().split("-")[1])) {
				residentDatas.add(val.toString().split("-")[0]);
			}
		}
		if(workDatas.size()<=0||residentDatas.size()<=0){
			log.error("未找到该用户的有效位置信息，用户Number："+ROW_KEY);
			return ;
		}
		Collections.sort(workDatas);//时间排序，list数据格式：timeStamp
		Collections.sort(residentDatas);
		String tw1 = workDatas.get(0);
		String tw2 = "";
		String tw3 = "";
		String th1 = residentDatas.get(0);
		String th2 = "";
		String th3 = "";
		
		 if (th1.compareTo("000000")>0 && th1.compareTo("040000")<0) {	// 白班
			tw2 = workDatas.get(workDatas.size()-1);
			for (String residentTime : residentDatas) {
				if (residentTime.compareTo(tw1)<0) {
					th2 = residentTime;
				} else if (residentTime.compareTo(tw2)>0) {
					th3 = residentTime;
					break;
				}
			}
			if(!"".equals(th2) && !"".equals(tw1) && !"".equals(tw2) && !"".equals(th3)){
				context.write(key, new Text("0-"+th2+"-"+tw1+"-"+tw2+"-"+th3));
			}
		}else if (tw1.compareTo("000000")>0 && tw1.compareTo("040000")<0) {	// 夜班
			th2 = residentDatas.get(residentDatas.size()-1);
			for (String workTime : workDatas) {
				if (workTime.compareTo(th1)<0) {
					tw2 = workTime;
				} else if (workTime.compareTo(th2)>0) {
					tw3 = workTime;
					break;
				}
			}
			if(!"".equals(tw2)&&!"".equals(th1)&&!"".equals(th2)&&!"".equals(tw3)){
				context.write(key, new Text("1-"+tw2+"-"+th1+"-"+th2+"-"+tw3)); 
			}
		}else{
			return;
		}
	}
	

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		hTable.close();
		log.info("the number of with null residentLacCellList is " + countR);
		log.info("the number of with null workLacCellListStr is " + countW);
	}

}
