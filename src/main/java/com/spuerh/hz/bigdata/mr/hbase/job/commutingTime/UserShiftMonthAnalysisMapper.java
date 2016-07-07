package com.spuerh.hz.bigdata.mr.hbase.job.commutingTime;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserShiftMonthAnalysisMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static Logger log = null;
	@Override
	protected void setup(Context context)
	{
		log = LoggerFactory.getLogger(UserShiftMonthAnalysisMapper.class);
		//map开始
		log.info("user shift month analysis mapper begin");
	}
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException,
			InterruptedException {
		String[] data = value.toString().split("\t");
		context.write(new Text(data[0].split("-")[0]), new Text(data[1]));
	}
}
