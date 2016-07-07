package com.spuerh.hz.bigdata.mr.gora.hbase.map;

import java.io.IOException;

import org.apache.gora.mapreduce.GoraMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.spuerh.hz.bigdata.mr.gora.hbase.persistentBase.Pageview;
import com.spuerh.hz.bigdata.mr.gora.hbase.tuple.TextLong;

public class PageviewMapper extends GoraMapper<Long, Pageview, TextLong,
      LongWritable> {
    
	/** The number of miliseconds in a day */
	private static final long DAY_MILIS = 1000 * 60 * 60 * 24;
	
    private LongWritable one = new LongWritable(1L);
  
    private TextLong tuple;
    
    @Override
    protected void setup(Context context) throws IOException ,InterruptedException {
      tuple = new TextLong();
      tuple.setKey(new Text());
      tuple.setValue(new LongWritable());
    };
    
    @Override
    protected void map(Long key, Pageview pageview, Context context)
        throws IOException ,InterruptedException {
      
      CharSequence url = pageview.getUrl();
      long day = getDay(pageview.getTimestamp());
      
      tuple.getKey().set(url.toString());
      tuple.getValue().set(day);
      
      context.write(tuple, one);
    };
    
    /** Rolls up the given timestamp to the day cardinality, so that 
     * data can be aggregated daily */
    private long getDay(long timeStamp) {
      return (timeStamp / DAY_MILIS) * DAY_MILIS; 
    }
}
