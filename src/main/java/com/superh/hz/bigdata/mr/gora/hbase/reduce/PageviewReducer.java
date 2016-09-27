package com.superh.hz.bigdata.mr.gora.hbase.reduce;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.io.LongWritable;

import com.superh.hz.bigdata.mr.gora.hbase.persistentBase.MetricDatum;
import com.superh.hz.bigdata.mr.gora.hbase.tuple.TextLong;


/**
 * The Reducer receives tuples of &lt;url, day&gt; as keys and a list of 
 * values corresponding to the keys, and emits a combined keys and
 * {@link MetricDatum} objects. The metric datum objects are stored 
 * as job outputs in the output data store.
 */
public class PageviewReducer extends GoraReducer<TextLong, LongWritable,
	      String, MetricDatum> {
	    
	    private MetricDatum metricDatum = new MetricDatum();
	    
	    @Override
	    protected void reduce(TextLong tuple, Iterable<LongWritable> values, Context context)
	      throws IOException ,InterruptedException {
	      
	      long sum = 0L; //sum up the values
	      for(LongWritable value: values) {
	        sum+= value.get();
	      }
	      
	      String dimension = tuple.getKey().toString();
	      long timestamp = tuple.getValue().get();
	      
	      metricDatum.setMetricDimension(new Utf8(dimension));
	      metricDatum.setTimestamp(timestamp);
	      
	      String key = metricDatum.getMetricDimension().toString();
	      key += "_" + Long.toString(timestamp);
	      metricDatum.setMetric(sum);
	      
	      context.write(key, metricDatum);
	    };
}
