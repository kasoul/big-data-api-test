package com.superh.hz.bigdata.api.redis.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.MultiKeyCommands;
import redis.clients.jedis.Transaction;

/**
 * redis client
 * 2015-4-8
 */
public class RedisClient {
	
	private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);
	
	public static void main(String args[]) {
		getRedisInstanceInfo();
	}

	public static void getRedisInstanceInfo() {
		Jedis jedis = new Jedis("127.0.0.1", 6379);
		System.out.println(jedis.exists("huangchaotest"));

	}

	public static void testSetAndGet() {
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		HostAndPort hostAndport = new HostAndPort("127.0.0.1", 6379);
		HostAndPort hostAndport2 = new HostAndPort("127.0.0.1", 6380);
		nodes.add(hostAndport);
		nodes.add(hostAndport2);
		JedisCluster jedisCluster = new JedisCluster(nodes);
		//jedisCluster.hset("DangDang:productid:to:categoryid:mapper", "field1", "test1");
		String hctestValue = jedisCluster.get("hctest");
		System.out.println(hctestValue == null);
	    jedisCluster.set("hctest","how are you ,man!");
		String hctestValue2 = jedisCluster.get("hctest");
		System.out.println(hctestValue2 == null);
		System.out.println(hctestValue2);
		long delflag = jedisCluster.del("hctest");
		System.out.println(delflag);
		//System.out.println(jedisCluster.hgetAll("DangDang:productid:to:categoryid:mapper"));
		//System.out.println(jedisCluster.del("DangDang:productid:to:categoryid:mapper"));
	}
	
	public static JedisCluster getClient(String ip, int port)
	{
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		HostAndPort hostAndport = new HostAndPort(ip, port);
		nodes.add(hostAndport);
		return new JedisCluster(nodes, 60000);
	}
	
	public static void multiandwatch(String ip, int port)
	{
		Jedis jedis = new Jedis("127.0.0.1", 6379);
		String watchedkey="test:watch:key";
		List<Object> list = null;
		while(list == null){
			jedis.watch(watchedkey);
			String completedNum = jedis.get(watchedkey);
			int increment = 1;
			String updatedcompletedNum = String.valueOf(Integer.parseInt(completedNum) + increment );
		
			Transaction tx = jedis.multi();
			tx.set(watchedkey,updatedcompletedNum);
			list = tx.exec();
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
			}
		}
		
		jedis.close();
	}

}
