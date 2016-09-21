/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.superh.hz.bigdata.api.hbase.tool.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Sample coprocessor endpoint exposing a Service interface for counting rows
 * and key values.
 * <p>
 * <p>
 * For the protocol buffer definition of the RowCountService, see the source
 * file located under hbase-server/src/main/protobuf/Examples.proto.
 * </p>
 */
public class CountEndpoint extends CountProtos.CountService
		implements Coprocessor, CoprocessorService {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());;
	private RegionCoprocessorEnvironment env;

	public CountEndpoint() {
	}

	/**
	 * Just returns a reference to this object, which implements the
	 * RowCounterService interface.
	 */
	@Override
	public Service getService() {
		return this;
	}

	/**
	 * Stores a reference to the coprocessor environment provided by the
	 * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from
	 * the region where this coprocessor is loaded. Since this is a coprocessor
	 * endpoint, it always expects to be loaded on a table region, so always
	 * expects this to be an instance of {@link RegionCoprocessorEnvironment}.
	 *
	 * @param env
	 *            the environment provided by the coprocessor host
	 * @throws IOException
	 *             if the provided environment is not an instance of
	 *             {@code RegionCoprocessorEnvironment}
	 */
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		logger.info("start service...");
		logger.info("load count Endpoint successfully");
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			logger.error("Must be loaded on a table region!");
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		logger.info("stop service...");
	}

	@Override
	public void count(RpcController controller, CountProtos.CountRequest request,
			RpcCallback<CountProtos.CountResponse> done) {
		//byte[] regionStartRow = env.getRegionInfo().getStartKey();
		//byte[] regionEndRow = env.getRegionInfo().getEndKey();
		InternalScanner scanner = null;
		CountProtos.CountResponse.Builder respBuilder = CountProtos.CountResponse.newBuilder();  
		if (!"count".equals(request.getCountFlag())) {  
            respBuilder.setCountValue(0);  
        }else {  
            long count = 0;  
            try {  
                Scan scan = new Scan();  
                scan.setMaxVersions(1);  
                scanner = env.getRegion().getScanner(scan);  
                List<Cell> list = new ArrayList<>();  
                while (scanner.next(list))  
                    count += 1;  
                respBuilder.setCountValue(count);  
            } catch (IOException e) {  
                e.printStackTrace();  
            } finally {  
                if (scanner != null)  
                    try {  
                        scanner.close();  
                    } catch (IOException e) {  
                        e.printStackTrace();  
                    }  
            }  
        }  
        done.run(respBuilder.build()); 
		
	}
}
