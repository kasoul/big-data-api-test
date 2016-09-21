package com.superh.hz.bigdata.api.hbase.tool.coprocessor;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

/**
 * count coprocessor 调用接口
 */
public class CountCoprocClient {
    private Connection connection;

    public CountCoprocClient(Connection connection) {
        this.connection = connection;
    }

    /**
     * 统计行数
     * @param String 传入参数：表名
     * @return long 统计值
     * @throws Throwable
     */
    public long countTable(String tableName) {

        Table table = null;
		try {
			table = connection.getTable(TableName
			        .valueOf(tableName));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        final CountProtos.CountRequest request = CountProtos.CountRequest.newBuilder()
                .setCountFlag("count")
                .build();

        final LongWritable totalCount = new LongWritable(0);
        try {
            table.coprocessorService(
            		CountProtos.CountService.class,
                    null,
                    null,
                    new Batch.Call<CountProtos.CountService, CountProtos.CountResponse>() {
                        public CountProtos.CountResponse call(CountProtos.CountService service)
                                throws IOException {
                            ServerRpcController controller = new ServerRpcController();
                            BlockingRpcCallback<CountProtos.CountResponse> rpcCallback = new BlockingRpcCallback<>();
                            service.count(controller, request,rpcCallback);
                            CountProtos.CountResponse response = rpcCallback.get();
                            if (controller.failedOnException()) {
                                throw controller.getFailedOn();
                            }
                            return response;
                        }
                    }, new Batch.Callback<CountProtos.CountResponse>() {
                        @Override
                        public void update(byte[] region, byte[] row, CountProtos.CountResponse response) {
                            System.out.println(Bytes.toString(region) + ","
                                    + Bytes.toString(row) + ", count number:"
                                    + response.getCountValue());
                            totalCount.set(totalCount.get() + response.getCountValue());
                        }

                    });
        } catch (ServiceException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return totalCount.get();
    }
}
