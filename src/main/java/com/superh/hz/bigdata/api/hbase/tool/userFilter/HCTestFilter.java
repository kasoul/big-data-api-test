package com.superh.hz.bigdata.api.hbase.tool.userFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 *  测试filter
 *  2015-2-3
 */
public class HCTestFilter extends FilterBase {

	private boolean filterRow = true;
	
	public Filter.ReturnCode filterKeyValue(KeyValue ignored){
	    if (!this.filterRow) {
	      return Filter.ReturnCode.INCLUDE;
	    }
	    return Filter.ReturnCode.NEXT_ROW;
	}

	public boolean filterRowKey(byte[] buffer, int offset, int length){
	    byte[] rowKey = Arrays.copyOfRange(buffer, offset, offset + length);
	    String str = new String(rowKey);
	   
	    if (str.contains("18867109997")){
	        this.filterRow = false;
	    }
	    return this.filterRow;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ReturnCode filterKeyValue(Cell arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	
}
