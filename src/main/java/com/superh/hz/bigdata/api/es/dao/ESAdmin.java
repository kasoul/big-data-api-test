package com.superh.hz.bigdata.api.es.dao;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * elastic search 索引创建工具
 */
public class ESAdmin implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ESAdmin.class);
	
	private Client client;
	private Settings settings;
	
	/**
	 * constructor
	 * 
	 * @param String,ip地址或主机名
	 * @param String,集群名称
	 * @param Map<String,Object>,客户端配置
	 */
	public ESAdmin(String ipAddress, String clusterName, Map<String,Object> settingConfig){
		
		this(ipAddress, clusterName, 9300, settingConfig);
		
	}

	/**
	 * constructor
	 * 
	 * @param String,ip地址或主机名
	 * @param String,集群名称
	 */
	public ESAdmin(String ipAddress, String clusterName){
		
		this(ipAddress, clusterName, 9300, null);
		
	}
	
	/**
	 * constructor
	 * 
	 * @param String,ip地址或主机名
	 * @param String,集群名称
	 * @param int,端口号
	 * @param Map<String,Object>,客户端配置
	 */
	public ESAdmin(String ipAddress, String clusterName,int transportPort, Map<String,Object> settingConfig) {

		// 通过连接的客户端
		Builder settingBuilder = Settings.settingsBuilder()
				.put("client.transport.sniff", true);
				
		if (clusterName != null && !clusterName.isEmpty()) {
			settingBuilder.put("cluster.name", clusterName);
		}

		if (settingConfig != null){
			for(String key:settingConfig.keySet()){
				settingBuilder.put(key,settingConfig.get(key));
			}
		}else{
			settingBuilder.put("max_result_window", 1000000);
			settingBuilder.put("number_of_shards", 4);
			settingBuilder.put("number_of_replicas", 1);
		}

		this.settings = settingBuilder.build();
		String[] ipAddress_array = ipAddress.split(",");
		List<InetSocketTransportAddress> transportAddressList = new ArrayList<InetSocketTransportAddress>();
		for(String host:ipAddress_array){
			try {
				transportAddressList.add(new InetSocketTransportAddress(InetAddress
						.getByName(host), transportPort));
			} catch (UnknownHostException e) {
				logger.error("can't find es server host!",e);
			}
		}
		
		client = TransportClient
				.builder()
				.settings(settings)
				.build()
				.addTransportAddresses(transportAddressList.toArray(new InetSocketTransportAddress[transportAddressList.size()]));
		
	}

	
	/**
	 * 获取client
	 */
	public Client getClient() {
		return client;
	}

	/**
	 * 建立索引
	 * 
	 * @param String,索引名称
	 * @param String,类型名称
	 * @param String,mapping json
	 * @return boolean,是否创建成功
	 */
	public boolean createIndex(String indexName,String typeName,String mappingTemp) {	
		CreateIndexResponse createIndexResponse = client.admin().indices()
				.prepareCreate(indexName)
				.setSettings(settings)
				.addMapping(typeName, mappingTemp)
				.execute()
				.actionGet();
		return createIndexResponse.isAcknowledged();
	}
	
	/**
	 * 判断索引是否存在
	 * 
	 * @param String,索引名称
	 * @return boolean,索引是否存在
	 */
	public boolean existIndex(String indexName) {
		IndicesExistsResponse indicesExistsResponse = client.admin().indices()
		.prepareExists(indexName)
		.execute()
		.actionGet();
		return indicesExistsResponse.isExists();
	}
	
	/**
	 * 删除索引
	 * 
	 * @param String,索引名称
	 * @return boolean,是否删除成功
	 */
	public boolean deleteIndex(String indexName) {
		DeleteIndexResponse deleteIndexResponse = client.admin().indices()
		.prepareDelete(indexName)
		.execute()
		.actionGet();
		return deleteIndexResponse.isAcknowledged();
	}
	
	
	/**
	 * 关闭该连接
	 */
	public void close(){
		
		client.close();
		
	}
	
}