package com.spuerh.hz.bigdata.mr.util.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  @Describe: 日期处理工具
 */
public class DateUtil {
	
	private static Logger log = LoggerFactory.getLogger(DateUtil.class);
	
	public static final SimpleDateFormat sdf_yyyyMMddhhmmss = new SimpleDateFormat("yyyyMMddHHmmss");
	public static final SimpleDateFormat sdf_yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
	public static final SimpleDateFormat sdf_yyyyMM = new SimpleDateFormat("yyyyMM");
	public static final SimpleDateFormat sdf_hhmmss = new SimpleDateFormat("HHmmss");
	
	/**
	 * 获取计算起始、终止的日期
	 * @param calDays 计算天数
	 * @param calDate 计算起始日期
	 * @return
	 */
	public static String[] getDateRange(int calDays, String calDate)
	{		
		Calendar calendar = Calendar.getInstance();
		if (calDate != null && !calDate.equals("null") && !calDate.equals(""))		
		{
			try {
				calendar.setTime(sdf_yyyyMMdd.parse(calDate));
			} catch (ParseException e) {
				log.error("date format is not illegal!", e);
			}
		}		
		
		String[] dateReturn = new String[3];
		calendar.add(Calendar.DATE, -1);
		dateReturn[0] = sdf_yyyyMMdd.format(calendar.getTime());//计算最晚日期，默认昨天
		
		calendar.add(Calendar.DATE, 1);
		calendar.add(Calendar.DATE, -1 * calDays);
		dateReturn[1] = sdf_yyyyMMdd.format(calendar.getTime());//根据计算天数，获得计算最早日期
		
		calendar.add(Calendar.DATE, calDays);
		calendar.add(Calendar.MONTH, -12);
		dateReturn[2] = sdf_yyyyMMdd.format(calendar.getTime());//计算结果过期日期

		//System.out.println(dateReturn[0] + ":" + dateReturn[1] + ":" + dateReturn[2]);
		return dateReturn;
	}

}
