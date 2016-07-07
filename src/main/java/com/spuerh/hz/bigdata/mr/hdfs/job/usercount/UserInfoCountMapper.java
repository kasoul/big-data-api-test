package com.spuerh.hz.bigdata.mr.hdfs.job.usercount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserInfoCountMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static Logger log = LoggerFactory.getLogger(UserInfoCountMapper.class);
	
	@Override
	protected void setup(Context context)
	{
		//map开始
		log.info("user info count mapper begin");
		
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException,
			InterruptedException {
		
		String[] data = value.toString().split("\t");
		context.write(new Text(data[0].split("-")[0]), new Text(data[1]));
		
	}
	
}
