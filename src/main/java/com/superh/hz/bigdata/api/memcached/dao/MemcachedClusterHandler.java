package com.superh.hz.bigdata.api.memcached.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;


/**
 * memcached 多实例客户端 client
 * 2017-2-21
 */
public class MemcachedClusterHandler {
	
	private static final Logger logger = LoggerFactory.getLogger(MemcachedClusterHandler.class);
	
	public static void main(String args[]) {
		initMemcachedSocketPool();
		testWriteMemcachedClient();
		testReadMemcachedClient();
	}

	public static void initMemcachedSocketPool() {
		//缺省构造的poolName是default。如果在客户端配置多个memcached服务，一定要显式声明poolName。
		SockIOPool stp = SockIOPool.getInstance("MemCachedClient1");
		
		//设置连接池可用的cache服务器列表，server的构成形式是IP:PORT
		stp.setServers(new String[]{"192.168.182.138:11211","192.168.182.140:11211"});
		
		//设置连接池可用cache服务器的权重，和server数组的位置一一对应
		//其实现方法是通过根据每个权重在连接池的bucket中放置同样数目的server（如下代码所示），因此所有权重的最大公约数应该是1，不然会引起bucket资源的浪费。 
		stp.setWeights(new Integer[]{1,5});
		
		//设置开始时每个cache服务器的可用连接数
		stp.setInitConn(3);
		
		//设置每个服务器最少可用连接数
		stp.setMaxConn(3);
		
		//设置每个服务器最大可用连接数
		stp.setMaxConn(10);
		
		//设置可用连接池的最长等待时间
		stp.setMaxIdle(5000);
		
		//设置连接池维护线程的睡眠时间
		//设置为0，维护线程不启动
		//维护线程主要通过log输出socket的运行状况，监测连接数目及空闲等待时间等参数以控制连接创建和关闭。
		stp.setMaintSleep(10000);
		
		/**
		 * 设置hash算法
		 * alg=0 使用String.hashCode()获得hash code,该方法依赖JDK，可能和其他客户端不兼容，建议不使用
		 * alg=1 使用original 兼容hash算法，兼容其他客户端
		 * alg=2 使用CRC32兼容hash算法，兼容其他客户端，性能优于original算法
		 * alg=3 使用MD5 hash算法 
		 * 采用前三种hash算法的时候，查找cache服务器使用余数方法。采用最后一种hash算法查找cache服务时使用consistent方法。
		 */
		stp.setHashingAlg(1);
		stp.setNagle(false);
		
		stp.setAliveCheck(true);
		
		//设置完pool参数后最后调用该方法，启动pool。
		stp.initialize();


	}
	
	public static void testWriteMemcachedClient() {
		MemCachedClient mcc = new MemCachedClient("MemCachedClient1");
		//mcc.setCompressEnable(true);  
		//mcc.setCompressThreshold(1000*1024);  
		for(String servername:mcc.stats().keySet()){
			System.out.println(servername);
			System.out.println(mcc.get(servername));
		}
		
		boolean addfalg1 = mcc.add("testclusterkey1", "this is test data,clusterkey1.");
		boolean addfalg2 = mcc.add("testclusterkey2", "this is test data,clusterkey2.");
		boolean addfalg3 = mcc.add("testclusterkey3", "this is test data,clusterkey3.");
		boolean addfalg4 = mcc.add("testclusterkey4", "this is test data,clusterkey4.");
		boolean addfalg5 = mcc.add("testclusterkey5", "this is test data,clusterkey5.");
		
		System.out.println(addfalg1);
		System.out.println(addfalg2);
		System.out.println(addfalg3);
		System.out.println(addfalg4);
		System.out.println(addfalg5);
	}
	
	public static void testReadMemcachedClient() {
		MemCachedClient mcc = new MemCachedClient("MemCachedClient1");

		System.out.println(mcc.get("testclusterkey1"));
		System.out.println(mcc.get("testclusterkey2"));
		System.out.println(mcc.get("testclusterkey3"));
		System.out.println(mcc.get("testclusterkey4"));
		System.out.println(mcc.get("testclusterkey5"));

		
	}

}
