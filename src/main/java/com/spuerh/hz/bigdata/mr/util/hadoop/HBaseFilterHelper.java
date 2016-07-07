package com.spuerh.hz.bigdata.mr.util.hadoop;

import java.util.Calendar;

import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

/** 
 * 自定义filter
 */
public class HBaseFilterHelper 
{
	public static RowFilter filterByDayRange(String startDay, String endDay)
	{
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, Integer.parseInt(startDay.substring(0, 4)));
		calendar.set(Calendar.MONTH, Integer.parseInt(startDay.substring(4, 6)) - 1);
		calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(startDay.substring(6, 8)));
		
		StringBuffer sBuffer = new StringBuffer();
		while (startDay.compareTo(endDay) <= 0)
		{
			if (sBuffer.length() == 0)
				sBuffer.append("[0-9]{11}-").append(startDay).append("[0-9]{6}");
			else
				sBuffer.append("|[0-9]{11}-").append(startDay).append("[0-9]{6}");
			
			calendar.add(Calendar.DAY_OF_MONTH, 1);

			startDay = String.format("%04d", calendar.get(Calendar.YEAR)) + 
					   String.format("%02d", calendar.get(Calendar.MONTH) + 1) + 
					   String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH));
		}
		return new RowFilter(CompareOp.EQUAL, new RegexStringComparator(sBuffer.toString()));
	}
	
	public static RowFilter filterByTimeRange(String startTime, String endTime)
	{
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(startTime.substring(0, 2)));
		calendar.set(Calendar.MINUTE, Integer.parseInt(startTime.substring(2, 4)));
		calendar.set(Calendar.SECOND, Integer.parseInt(startTime.substring(4, 6)));
		
		StringBuffer sBuffer = new StringBuffer();
		while (startTime.compareTo(endTime) < 0)
		{
			if (sBuffer.length() == 0)
				sBuffer.append("[0-9]{11}-[0-9]{8}").append(startTime.substring(0, 2)).append("[0-9]{4}");
			else
				sBuffer.append("|[0-9]{11}-[0-9]{8}").append(startTime.substring(0, 2)).append("[0-9]{4}");
			
			calendar.add(Calendar.HOUR_OF_DAY, 1);

			startTime = String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY)) + 
					   	String.format("%02d", calendar.get(Calendar.MINUTE)) + 
					   	String.format("%02d", calendar.get(Calendar.SECOND));
		}
		return new RowFilter(CompareOp.EQUAL, new RegexStringComparator(sBuffer.toString()));
	}
	
	public static RowFilter filterByDayAndTimeRange(String startDay, String endDay, String startTime, String endTime)
	{		
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, Integer.parseInt(startDay.substring(0, 4)));
		calendar.set(Calendar.MONTH, Integer.parseInt(startDay.substring(4, 6)) - 1);
		calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(startDay.substring(6, 8)));
		calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(startTime.substring(0, 2)));
		calendar.set(Calendar.MINUTE, Integer.parseInt(startTime.substring(2, 4)));
		calendar.set(Calendar.SECOND, Integer.parseInt(startTime.substring(4, 6)));
		
		String startTimeBackup = startTime;
		
		StringBuffer sBuffer = new StringBuffer();
		while (startDay.compareTo(endDay) <= 0)
		{
			while (startTime.compareTo(endTime) < 0)
			{
				if (sBuffer.length() == 0)
					sBuffer.append("[0-9]{11}-").append(startDay).append(startTime.substring(0, 2)).append("[0-9]{4}");
				else
					sBuffer.append("|[0-9]{11}-").append(startDay).append(startTime.substring(0, 2)).append("[0-9]{4}");
				
				calendar.add(Calendar.HOUR_OF_DAY, 1);

				startTime = String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY)) + 
						   	String.format("%02d", calendar.get(Calendar.MINUTE)) + 
						   	String.format("%02d", calendar.get(Calendar.SECOND));
			}
			startTime = startTimeBackup;
			calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(startTime.substring(0, 2)));
			calendar.set(Calendar.MINUTE, Integer.parseInt(startTime.substring(2, 4)));
			calendar.set(Calendar.SECOND, Integer.parseInt(startTime.substring(4, 6)));
			
			calendar.add(Calendar.DATE, 1);

			startDay = String.format("%04d", calendar.get(Calendar.YEAR)) + 
					   String.format("%02d", calendar.get(Calendar.MONTH) + 1) + 
					   String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH));			
		}
		return new RowFilter(CompareOp.EQUAL, new RegexStringComparator(sBuffer.toString()));
	}

}
