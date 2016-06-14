package cmcc.zyhy.hbase.datainsert.lbs.userpositiontable;

import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserPositionProducerRun {
	private static final Logger LOG = LoggerFactory.getLogger(UserPositionProducerRun.class);

	public static void main(String[] args) {
		insert30DaysAnd500UsersTest();

	}

	private static void insertSingleThread() {
		ExecutorService executor = Executors.newFixedThreadPool(50);
		for (int x = 0; x < 50; x++) {
			Calendar cal = Calendar.getInstance();
			executor.execute(new ProducerThread30DayssAnd500Users(cal, x));
		}
	}

	// 30天500个用户测试
	private static void insert30DaysAnd500UsersTest() {
		LOG.info("===============Begin insert data: 30 days,500 users.===============");
		ExecutorService executor = Executors.newFixedThreadPool(31);
		for (int x = -30; x < 1; x++) {
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, x);
			executor.execute(new ProducerThread30DayssAnd500Users(cal, x));
		}
		executor.shutdown();
		// 请求关闭、发生超时或者当前线程中断，无论哪一个首先发生之后，都将导致阻塞，直到所有任务完成执行
		// executor.awaitTermination(10, TimeUnit.SECONDS);
		while (true) {
			if (executor.isTerminated()) {
				// 如果没有while(true)，则可能在子线程还未关闭，因为shutdown()方法发出的异步指令
				LOG.info("===============End insert data: 30 days,500 users.===============");
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
