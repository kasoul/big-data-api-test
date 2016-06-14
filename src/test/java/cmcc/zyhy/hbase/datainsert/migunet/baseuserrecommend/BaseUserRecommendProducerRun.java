package cmcc.zyhy.hbase.datainsert.migunet.baseuserrecommend;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试数据插入，咪咕音乐项目-用户推荐表
 * 
 */
public class BaseUserRecommendProducerRun {
	private static final Logger LOG = LoggerFactory.getLogger(BaseUserRecommendProducerRun.class);

	public static void main(String[] args) {
		insert5000wUsersTest();

	}

	// 5000W个用户测试
	private static void insert5000wUsersTest() {
		LOG.info("===============Begin insert 5000w users.===============");
		ExecutorService executor = Executors.newFixedThreadPool(50);
		for (int x = 0; x < 50; x++) {
			executor.execute(new ProducerThread5000WAnd12Month(x));
		}
		executor.shutdown();
		// 请求关闭、发生超时或者当前线程中断，无论哪一个首先发生之后，都将导致阻塞，直到所有任务完成执行
		// executor.awaitTermination(10, TimeUnit.SECONDS);
		while (true) {
			if (executor.isTerminated()) {
				// 如果没有while(true)，则可能在子线程还未关闭，因为shutdown()方法发出的异步指令
				LOG.info("===============End insert 5000w users.===============");
				break;
			}
			try {
				Thread.sleep(2000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
