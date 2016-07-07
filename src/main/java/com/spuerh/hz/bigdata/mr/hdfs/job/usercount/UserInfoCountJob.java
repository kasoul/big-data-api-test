package com.spuerh.hz.bigdata.mr.hdfs.job.usercount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spuerh.hz.bigdata.mr.util.hadoop.ConfUtil;
import com.spuerh.hz.bigdata.mr.util.hadoop.ToolRunnerAgrs;


/**
 *  @Describe:用户数量统计job
 */
public class UserInfoCountJob  extends Configured implements Tool{
	private static Logger log = LoggerFactory.getLogger(UserInfoCountJob.class);
	private static String inputPath = "/user/hive/warehouse/hive_user_info_table";
	private static String outputPath = "/tmp/userinfo";

	public static void main(String[] args) throws Exception {
		//设置log level
		
		log.info("---------------------------------submit job---------------------------------");
		Configuration conf = ConfUtil.getBussinesConf("UserInfoCount");
		ToolRunner.run(conf, new UserInfoCountJob(), ToolRunnerAgrs.getAgrs());
	}
	
	public int run(String[] args) throws Exception 
	{
		//配置信息
		Configuration conf = getConf();
		
		FileSystem HDFS = FileSystem.get(conf);
		String HDFS_URI = HDFS.getUri().toString();

		log.info("hdfs uri [{}]", HDFS_URI);

		if(!HDFS.exists(new Path(inputPath))){
			log.error("job input path is not exists ");
			return 1;
		}
		
		if(HDFS.exists(new Path(outputPath))){
			log.error("job output path is exists ");
			return 1;
		}
		
		//job信息
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserInfoCountJob.class);
		job.setJobName("user info record count job");
		
		job.setMapperClass(UserInfoCountMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		
		job.setReducerClass(UserInfoCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job,new Path(outputPath));
	
		
		int reduceTaskNum = conf.getInt("mapreduce.reduce.task.number",1);
		job.setNumReduceTasks(reduceTaskNum);

		return (job.waitForCompletion(true) ? 0 : 1);
		
	}
}
