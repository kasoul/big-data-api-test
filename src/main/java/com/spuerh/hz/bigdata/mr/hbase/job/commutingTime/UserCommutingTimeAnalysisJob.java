package com.spuerh.hz.bigdata.mr.hbase.job.commutingTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spuerh.hz.bigdata.mr.hbase.job.hbaseSchema.LbsResultTable;
import com.spuerh.hz.bigdata.mr.hbase.job.hbaseSchema.UserPositionTable;
import com.spuerh.hz.bigdata.mr.util.common.DateUtil;
import com.spuerh.hz.bigdata.mr.util.hadoop.ConfUtil;
import com.spuerh.hz.bigdata.mr.util.hadoop.JobConstants;
import com.spuerh.hz.bigdata.mr.util.hadoop.ToolRunnerAgrs;


/**
 *  @Describe:用户通勤时段计算job
 */
public class UserCommutingTimeAnalysisJob extends Configured implements Tool{
	private static Logger log = LoggerFactory.getLogger(UserCommutingTimeAnalysisJob.class);
	
	private static String tempPath = "/lbs_temp/commuteTime/temp";
	
	public static void main(String[] args) throws Exception{
		
		log.info("---------------------------------submit job---------------------------------");
		Configuration conf = ConfUtil.getBussinesConf("UserCommutingTimeAnalysis");
		ToolRunner.run(conf, new UserCommutingTimeAnalysisJob(), ToolRunnerAgrs.getAgrs());
	}
	
	public int run(String[] args) throws Exception 
	{
		//配置信息
		Configuration conf = getConf();
		
		FileSystem HDFS = FileSystem.get(conf);
		String HDFS_URI = HDFS.getUri().toString();

		log.info("hdfs uri [{}]", HDFS_URI);

		
		String calDate = conf.get("UserCommutingTimeAnalysis.calculate.date");
		int calDays = conf.getInt("UserCommutingTimeAnalysis.calculate.days", 30);
		
		String[] date = DateUtil.getDateRange(calDays, calDate);
		conf.set(JobConstants.CURRENT_DATE_PROPERTY_NAME, date[0]);
		conf.set(JobConstants.BEGIN_DATE_PROPERTY_NAME, date[1]);
		
		//--------	第一阶段job---------------
		//job信息
		Job job1 = Job.getInstance(conf, "job1");
		job1.setJarByClass(UserCommutingTimeAnalysisJob.class);
		job1.setJobName("user commuting time analysis job1" + "--day:" + date[1] + "-" + date[0]);
		
		//设置HBase表的过滤
		Scan scan = new Scan();
		//根据不同的计算模型，设置不同的过滤时间范围
		scan.setCacheBlocks(false);
		int cachingNum = conf.getInt("hbase.scan.cachingNum", 2500);
		scan.setCaching(cachingNum);
		scan.addColumn(UserPositionTable.CF_POSITION, UserPositionTable.QL_LACCELL);
		scan.addColumn(UserPositionTable.CF_POSITION, UserPositionTable.QL_DURATION);
		
		//设置map
		TableMapReduceUtil.initTableMapperJob(UserPositionTable.TABLE_NAME, 
											  scan, 
											  UserShiftDayAnalysisMapper.class, 
											  Text.class, 
											  Text.class, 
											  job1);
		job1.setReducerClass(UserShiftDayAnalysisReducer.class);
		int reduceTaskNum = conf.getInt("mapreduce.reduce.task.number",1);
		job1.setNumReduceTasks(reduceTaskNum);
		FileOutputFormat.setOutputPath(job1,new Path(tempPath));
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		//--------	第二阶段job---------------
		Job job2 = Job.getInstance(conf, "job2");
		job2.setJarByClass(UserCommutingTimeAnalysisJob.class);
		job2.setJobName("user commuting time analysis job2" + "--day:" + date[1] + "-" + date[0]);
		job2.setMapperClass(UserShiftMonthAnalysisMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(tempPath));
		//设置reduce
		TableMapReduceUtil.initTableReducerJob(LbsResultTable.TABLE_NAME, 
				UserShiftMonthAnalysisReducer.class, 
				   job2);
		job2.setNumReduceTasks(reduceTaskNum);

		//--------	执行两个阶段的job---------------
		int result;
		if(job1.waitForCompletion(true)){
			result= (job2.waitForCompletion(true) ? 0 : 1);
		}else{
			result= 1;
		}
		/*String PATH = "hdfs://mycluster/";
		String DIR = "/lbs_temp/Commute";
		FileSystem fileSystem;
		fileSystem = FileSystem.get(new URI(PATH), new Configuration());*/
		HDFS.delete(new Path(tempPath), true);
		return result;
	}
	
}
