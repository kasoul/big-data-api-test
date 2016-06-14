package cmcc.zyhy.hbase.datainsert.lbs.userpositiontable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;


/** 
 *
 * @Project: hbase-accessor
 * @File: RedisClient.java 
 * @Date: 2015年4月8日 
 * @Author: 黄超（huangchaohz）
 * @Copyright: 版权所有 (C) 2015 中国移动 杭州研发中心. 
 *
 * @注意：本内容仅限于中国移动内部传阅，禁止外泄以及用于其他的商业目的 
 */

/**
 * @Describe:
 */
public class RedisClient {
	
	private static final Logger log = LoggerFactory.getLogger(RedisClient.class);
	

	
	public static void main(String args[]) {
		getLaccellMap();
	}

	public static void getClusterInfo() {
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		HostAndPort hostAndport = new HostAndPort("192.168.11.218", 6379);
		nodes.add(hostAndport);
		JedisCluster jedisCluster = new JedisCluster(nodes);
		Map<String, JedisPool> map =jedisCluster.getClusterNodes();
		
		System.out.println(map);

	}

	public static void testSetAndGet() {
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		HostAndPort hostAndport = new HostAndPort("192.168.12.47", 6379);
		nodes.add(hostAndport);
		JedisCluster jedisCluster = new JedisCluster(nodes);
		String hctestValue = jedisCluster.get("hctest");
		System.out.println(hctestValue == null);
	    jedisCluster.set("hctest","how are you ,man!");
		String hctestValue2 = jedisCluster.get("hctest");
		System.out.println(hctestValue2 == null);
		System.out.println(hctestValue2);
		long delflag = jedisCluster.del("hctest");
		System.out.println(delflag);
	}
	
	public static Map<String,String> getLaccellMap() {
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		HostAndPort hostAndport = new HostAndPort("192.168.12.45", 6379);
		nodes.add(hostAndport);
		JedisCluster jedisCluster = new JedisCluster(nodes);
		Map<String, String> map = jedisCluster.hgetAll("lac:cellid:to:coordinates:mapper");
		System.out.println(map.size());
		System.out.println(map.get(map.keySet().iterator().next()).replace(":", ","));
		return map;

	}
}
