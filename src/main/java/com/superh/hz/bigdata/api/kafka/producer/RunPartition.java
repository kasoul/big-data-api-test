package com.superh.hz.bigdata.api.kafka.producer;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 分区类，生产者的消息根据这个类进入特定的分区。
 */
public class RunPartition implements Partitioner
{
	public RunPartition(VerifiableProperties props)
	{
	}

	/**
	 * 生产者的分区规则
	 */
	public int partition(Object key, int numPartitions) {
		// key 是指定的分区号
		if (key == null)
		{
			// 异常情况随机存储到partition
			Random random = new Random();
			return random.nextInt(numPartitions);
		}else{
			// 正常情况，通过hashcode区分存储位置
			int result = Math.abs(key.hashCode()) % numPartitions;
			return result;
		}
	}

}
