package com.superh.hz.bigdata.api.memcached.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;


/**
 * memcached 单实例客户端 client
 * 2017-2-21
 */
public class MemcachedHandler {
	
	private static final Logger logger = LoggerFactory.getLogger(MemcachedHandler.class);
	
	public static void main(String args[]) {
		initMemcachedSocketPool();
		//testWriteMemcachedClient();
		//testReadMemcachedClient();
		testReadMemcachedClient2();
	}

	public static void initMemcachedSocketPool() {
		//缺省构造的poolName是default。如果在客户端配置多个memcached服务，一定要显式声明poolName。
		SockIOPool stp = SockIOPool.getInstance("MemCachedClient1");
		
		//设置连接池可用的cache服务器列表，server的构成形式是IP:PORT
		stp.setServers(new String[]{"192.168.182.137:11211"});
		
		//设置连接池可用cache服务器的权重，和server数组的位置一一对应
		//其实现方法是通过根据每个权重在连接池的bucket中放置同样数目的server（如下代码所示），因此所有权重的最大公约数应该是1，不然会引起bucket资源的浪费。 
		stp.setWeights(new Integer[]{3});
		
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
		

		System.out.println(mcc.keyExists("testkey1"));
		boolean setfalg = mcc.set("testkey1", "I am Kasoul, test key1.");
		boolean addfalg = mcc.add("testkey2", "I am Kasoul, test key2.");
		boolean repalcefalg = mcc.replace("testkey3", "I am Kasoul, test key3.");
		System.out.println(setfalg);
		System.out.println(addfalg);
		System.out.println(repalcefalg);
	}
	
	public static void testReadMemcachedClient() {
		MemCachedClient mcc = new MemCachedClient("MemCachedClient1");

		Object ob1 = mcc.get("testkey1");
		Object ob2 = mcc.get("testkey2");
		Object ob3 = mcc.get("testkey3");
		System.out.println(ob1);
		System.out.println(ob2);
		System.out.println(ob3);
		
	}
	
	public static void testReadMemcachedClient2() {
		MemCachedClient mcc = new MemCachedClient("MemCachedClient1");

		System.out.println(mcc.get("testclusterkey1"));
		System.out.println(mcc.get("testclusterkey2"));
		System.out.println(mcc.get("testclusterkey3"));
		System.out.println(mcc.get("testclusterkey4"));
		System.out.println(mcc.get("testclusterkey5"));
		
	}

}
