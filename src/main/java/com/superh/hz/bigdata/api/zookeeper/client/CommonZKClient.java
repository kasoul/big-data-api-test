package com.superh.hz.bigdata.api.zookeeper.client;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;



/** 
 * @Project: big-apple-api-test
 * @File: CommonZKClient.java 
 * @Date: 2015年6月19日 
 * @Author: 黄超（huangchaohz）
 */

/**
 * @Describe:简单的zookeeper客户端
 */
public class CommonZKClient {
	public static void main(String[] args) {
		try {
			testGetRootZnode();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void testGetRootZnode() throws IOException, KeeperException, InterruptedException {
		 // 创建一个与服务器的连接
		 ZooKeeper zk = new ZooKeeper("192.168.12.48:" + "2181", 
		        3000, new Watcher() { 
		            // 监控所有被触发的事件
		            public void process(WatchedEvent event) { 
		                System.out.println("已经触发了" + event.getType() + "事件！"); 
		            }
		        }); 
		 // 查看根目录下所有的子节点
		 /*zk.create("/hctest/test1", "huangchao".getBytes(), Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		 
		 
		 Stat s1= zk.exists("/hctest", false);
		 Stat s2=zk.exists("/hctest/test1", false);
		 System.out.println(s1.getVersion());
		 System.out.println(s2.getVersion());
		 zk.delete("/hctest/test1", 0);
		 zk.delete("/hctest", 0);*/
		 System.out.println(zk.getChildren("/consumers",false));
		 System.out.println(zk.getData("/brokers/topics", false, null));  
		 
		 // 获取一个目录节点的数据
		 //System.out.println(new String(zk.getData("/hbase/master",true,null),"UTF-8")); 

		 // 关闭连接
		 zk.close();
	}

	public static void test() throws IOException, KeeperException, InterruptedException {
		 // 创建一个与服务器的连接
		 ZooKeeper zk = new ZooKeeper("192.168.16.2:" + "2181", 
		        30, new Watcher() { 
		            // 监控所有被触发的事件
		            public void process(WatchedEvent event) { 
		                System.out.println("已经触发了" + event.getType() + "事件！"); 
		            } 
		        }); 
		 // 创建一个目录节点
		 zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE,
		   CreateMode.PERSISTENT); 
		 // 创建一个子目录节点
		 zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
		   Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
		 System.out.println(new String(zk.getData("/testRootPath",false,null))); 
		 // 取出子目录节点列表
		 System.out.println(zk.getChildren("/testRootPath",true)); 
		 // 修改子目录节点数据
		 zk.setData("/testRootPath/testChildPathOne","modifyChildDataOne".getBytes(),-1); 
		 System.out.println("目录节点状态：["+zk.exists("/testRootPath",true)+"]"); 
		 // 创建另外一个子目录节点
		 zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(), 
		   Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
		 System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo",true,null))); 
		 // 删除子目录节点
		 zk.delete("/testRootPath/testChildPathTwo",-1); 
		 zk.delete("/testRootPath/testChildPathOne",-1); 
		 // 删除父目录节点
		 zk.delete("/testRootPath",-1); 
		 // 关闭连接
		 zk.close();
	}

}
