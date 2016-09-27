package com.superh.hz.bigdata.mr.hdfs.job.usercount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * reudce的key是用户id，设置成单个reduce任务，可以统计所有用户
 */
public class UserInfoCountReducer extends
		Reducer<Text, Text, Text, Text> {
	private static Logger log = LoggerFactory.getLogger(UserInfoCountReducer.class);
	
	private long sum_lines;


	@Override
	protected void setup(Context context){
		
		log.info("user info count reducer begin");
		
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		//String ROW_KEY = key.toString().split("-")[0];
		sum_lines++;
		
	}
	

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		log.info("the total number of users is ",sum_lines);
	}

}
