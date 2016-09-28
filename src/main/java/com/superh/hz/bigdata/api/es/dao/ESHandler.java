package com.superh.hz.bigdata.api.es.dao;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * elastic search 索引操作工具
 */
public class ESHandler implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ESHandler.class);
	
	private Client client;
	private String indexName;
	private String typeName;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * constructor
	 * 
	 * @param String,ip地址或主机名
	 * @param String,集群名称
	 * @param String,索引名称
	 * @param String,类型名称
	 */
	public ESHandler(String ipAddress, String clusterName,String indexName,String typeName){
		
		this(ipAddress, clusterName, 9300, indexName, typeName);
		
	}
	
	/**
	 * constructor
	 * 
	 * @param String,ip地址或主机名
	 * @param String,集群名称
	 * @param int,端口号
	 * @param String,索引名称
	 * @param String,类型名称
	 */
	public ESHandler(String ipAddress, String clusterName,int transportPort,
			String indexName,String typeName) {
		
		this.indexName = indexName;
		this.typeName = typeName;
		// 通过连接的客户端
		Builder settingBuilder = Settings.settingsBuilder().put(
				"client.transport.sniff", true);
		if (clusterName != null && !clusterName.isEmpty()) {
			settingBuilder.put("cluster.name", clusterName);
		}
		
		Settings settings = settingBuilder.build();
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
	 * 批量索引文档
	 * 
	 * @param List<Object>,文档列表
	 * @return BulkResponse,批量请求响应
	 */
	public BulkResponse bulkIndex(List<Object> documentList) {
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		// 创建索引库 需要注意的是.setRefresh(true)这里一定要设置,否则第一次建立索引查找不到数据
		for (Object document : documentList) {
			
			byte[] bytes = null;
			// 一部分被标记为@JsonIgnore的字段不会被生成到json数据里
			try {
				bytes = objectMapper.writeValueAsBytes(document);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}

			// setSource最终都是将参数转成byte[]类型，具体的参数性能：
			// XContentBuilder：取决于XContentBuilder.bytes()方法
			// String: 取决于String.getBytes()方法
			// Map<String,?>: 先将Map转成XContentBuilder,再调用XContentBuilder.bytes()方法
			// 故性能应该是String >= XContentBuilder > map
			
			bulkRequestBuilder.add(client
					.prepareIndex(indexName,typeName)
					.setSource(bytes));
		}
		
		return bulkRequestBuilder.execute().actionGet();
		
	}
	
	/**
	 * 批量索引文档，并设置文档id
	 * 
	 * @param Map<String,Object>,文档id和文档列表
	 * @return BulkResponse,批量请求响应
	 */
	public BulkResponse bulkIndexWithId(Map<String,Object> documentMap) throws JsonProcessingException  {
		
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

		for (String documentId : documentMap.keySet()) {
			
			byte[] bytes = null;
			bytes = objectMapper.writeValueAsBytes(documentMap.get(documentId));
			bulkRequestBuilder.add(client
					.prepareIndex(indexName,typeName)
					.setSource(bytes)
					.setId(documentId));
			
		}
		
		return bulkRequestBuilder.execute().actionGet();
		
	}

	
	/**
	 * 批量删除文档
	 * 
	 * @param List<String>,文档id列表
	 * @return BulkResponse,批量请求响应
	 */
	public BulkResponse bulkDelete(List<String> deleteIds) {
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (String deleteId : deleteIds) {

			bulkRequestBuilder.add(client.prepareDelete(indexName,typeName, deleteId));
		}
		return bulkRequestBuilder.execute().actionGet();
	}

	/**
	 * 索引一个文档
	 * 
	 * @param Map<String, Object>,map映射为json对象
	 * @return IndexResponse,请求响应
	 */
	public IndexResponse put(Map<String, Object> sourceData) {
		IndexResponse response = client.prepareIndex(indexName, typeName)
				.setSource(sourceData).execute().actionGet();
		return response;
	}

	/**
	 * 索引一个文档
	 * 
	 * @param String,json对象
	 * @return IndexResponse,请求响应
	 */
	public IndexResponse put(String jsondata) {
		IndexResponse response = client.prepareIndex(indexName, typeName)
				.setSource(jsondata).execute().actionGet();
		return response;
	}
	
	/**
	 * 索引一个文档
	 * 
	 * @param byte[],json对象
	 * @return IndexResponse,请求响应
	 */
	public IndexResponse put(byte[] bytes) {
		IndexResponse response = client.prepareIndex(indexName, typeName)
				.setSource(bytes).execute().actionGet();
		return response;
	}
	
	/**
	 * 索引一个文档
	 * 
	 * @param Object,object映射为json对象
	 * @return IndexResponse,请求响应
	 */
	public IndexResponse put(Object object) {
		IndexResponse response = null;
		try {
			response = client.prepareIndex(indexName, typeName)
					.setSource(objectMapper.writeValueAsBytes(object))
					.execute()
					.actionGet();
		} catch (JsonProcessingException e) {
			logger.error("parse object to json error!",e);
		}
		return response;
	}
	
	/**
	 * 索引一个文档,并设置文档id
	 * 
	 * @param Object,文档对象
	 * @param String,文档id
	 * @return IndexResponse,请求响应
	 */
	public IndexResponse put(Object object,String id) {
		
		IndexResponse response = null;
		
		try {
			response = client.prepareIndex(indexName, typeName)
					.setSource(objectMapper.writeValueAsBytes(object))
					.setId(id)
					.execute()
					.actionGet();
		} catch (JsonProcessingException e) {
			logger.error("parse object to json error!",e);
		}
		
		return response;
	}
	
	/**
	 * 删除一个文档
	 * 
	 * @param String,文档id
	 * @return DeleteResponse,请求响应
	 */
	public DeleteResponse delete(String id) {
		DeleteResponse response = client.prepareDelete(indexName, typeName,id)
					.execute()
					.actionGet();
		return response;
	}
	
	/**
	 * 批量索引文档，输入外部请求器，不提交到服务器
	 * 
	 * @param BulkRequestBuilder,请求构建器
	 * @param List<Object>,文档列表
	 */
	public void bulkIndex(BulkRequestBuilder bulkRequestBuilder,List<Object> documentList) throws JsonProcessingException  {

		for (Object document : documentList) {
			
			byte[] bytes = null;
			bytes = objectMapper.writeValueAsBytes(document);
			bulkRequestBuilder.add(client
					.prepareIndex(indexName,typeName)
					.setSource(bytes));
		}
		
	}
	
	/**
	 * 批量删除文档，输入外部请求器，不提交到服务器
	 * 
	 * @param BulkRequestBuilder,请求构建器
	 * @param List<String>,文档id列表
	 */
	public void bulkDelete(BulkRequestBuilder bulkRequestBuilder,List<String> deleteIds) {

		for (String deleteId : deleteIds) {

			bulkRequestBuilder.add(client.prepareDelete(indexName,typeName, deleteId));
		}

	}
	
	/**
	 * 提交批量请求
	 * 
	 * @param BulkRequestBuilder,请求构建器
	 * @return BulkResponse,批量请求响应
	 */
	public BulkResponse bulkAction(BulkRequestBuilder bulkRequestBuilder){
		
		return bulkRequestBuilder.execute().actionGet();
		
		
	}
	
	/**
	 * 关闭该连接
	 */
	public void close(){
		
		client.close();
		
	}
	
}