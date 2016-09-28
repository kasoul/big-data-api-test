package com.superh.hz.bigdata.api.hbase.dao;

/**
 * hbase表的region预分区器接口
 */
public interface RegionPreSpliter {
	
    /**
     * 生成预分区的所有region startkey和endkey信息
     *
     * @return
     */
    byte[][] calcSplitKeys(int regionCount);
}
