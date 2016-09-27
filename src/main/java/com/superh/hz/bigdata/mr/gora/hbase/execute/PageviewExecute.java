package com.superh.hz.bigdata.mr.gora.hbase.execute;

import java.io.IOException;

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.superh.hz.bigdata.mr.gora.hbase.map.PageviewMapper;
import com.superh.hz.bigdata.mr.gora.hbase.persistentBase.MetricDatum;
import com.superh.hz.bigdata.mr.gora.hbase.persistentBase.Pageview;
import com.superh.hz.bigdata.mr.gora.hbase.reduce.PageviewReducer;
import com.superh.hz.bigdata.mr.gora.hbase.tuple.TextLong;

/**
 * @author gora example
 *
 */
public class PageviewExecute extends Configured implements Tool {

	private static final Logger log = LoggerFactory.getLogger(PageviewExecute.class);

	public Job createPageViewJob(DataStore<Long, Pageview> inStore,
			DataStore<String, MetricDatum> outStore, int numReducer)
			throws IOException {

		Job job = new Job(getConf());
		job.setJobName("Page View Analytics");
		log.info("Creating Hadoop Job: " + job.getJobName());
		job.setNumReduceTasks(numReducer);
		job.setJarByClass(getClass());

		GoraMapper.initMapperJob(job, inStore, TextLong.class,
				LongWritable.class, PageviewMapper.class, true);

		GoraReducer.initReducerJob(job, outStore, PageviewReducer.class);

		return job;
	}

	public int run(String[] args) throws Exception {

		DataStore<Long, Pageview> inStore;
		DataStore<String, MetricDatum> outStore;
		Configuration conf = new Configuration();

		if (args.length > 0) {
			String dataStoreClass = args[0];
			//dataStoreClass = "org.apache.gora.hbase.store.HBaseStore"
			inStore = DataStoreFactory.getDataStore(dataStoreClass, Long.class,
					Pageview.class, conf);
			if (args.length > 1) {
				dataStoreClass = args[1];
			}
			outStore = DataStoreFactory.getDataStore(dataStoreClass,
					String.class, MetricDatum.class, conf);
		} else {
			inStore = DataStoreFactory.getDataStore(Long.class, Pageview.class,
					conf);
			outStore = DataStoreFactory.getDataStore(String.class,
					MetricDatum.class, conf);
		}

		Job job = createPageViewJob(inStore, outStore, 3);
		boolean success = job.waitForCompletion(true);

		inStore.close();
		outStore.close();

		log.info("Log completed with " + (success ? "success" : "failure"));

		return success ? 0 : 1;
	}

	private static final String USAGE = "LogAnalytics <input_data_store> <output_data_store>";

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println(USAGE);
			System.exit(1);
		}
		// run as any other MR job
		int ret = ToolRunner.run(new PageviewExecute(), args);
		System.exit(ret);
	}

}
