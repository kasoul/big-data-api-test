package com.superh.hz.bigdata.api.es.dao;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * elastic search 索引查询工具
 */
public class ESSearcher implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(ESSearcher.class);
	
	private Client client;
	private String indexName;
	private String typeName;

	/**
	 * constructor
	 * 
	 * @param String,ip地址或主机名
	 * @param String,集群名称
	 * @param String,索引名称
	 * @param String,类型名称
	 */
	public ESSearcher(String ipAddress, String clusterName,String indexName,String typeName){
		
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
	public ESSearcher(String ipAddress, String clusterName,int transportPort,
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
	 * 根据id查询一个文档
	 * 
	 * @param String,文档id
	 * @return Map<String, Object>,文档映射为map
	 */
	public Map<String, Object> getDocToMap(String id) {
		GetResponse getResponse = client.prepareGet(indexName, typeName, id)
				.execute()
				.actionGet();
		return getResponse.getSource();
	}
	
	/**
	 * 根据多个id查询文档
	 * 
	 * @param Iterable<String>,文档id集合
	 * @return List<Map<String, Object>>,文档集合
	 */
	public List<Map<String, Object>> getDocsToMap(Iterable<String> ids) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		MultiGetResponse multiGetResponse = client.prepareMultiGet()
				 .add(indexName, typeName, ids)
				 .execute()
				 .actionGet();
		Iterator<MultiGetItemResponse> iterator = multiGetResponse.iterator();
		while (iterator.hasNext()){
			MultiGetItemResponse multiGetItemResponse = multiGetResponse.iterator().next();
			list.add(multiGetItemResponse.getResponse().getSource());
		}
		
		
		return list;
		
	}

	
	/**
	 * 扫描一个索引的所有文档
	 * 
	 * @param String... fields,需要返回的字段
	 * @return List<Map<String, Object>>,查询结果，文档集合
	 */
	public List<Map<String, Object>> scan(String... fields ) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				.addFields(fields)
				.execute()
				.actionGet();
		SearchHit[] hits = searchResponse.getHits().getHits();
		List<Map<String,Object>> list_res = new ArrayList<Map<String,Object>>() ;
		if(hits !=null && hits.length > 0){
			for (int i = 0; i < hits.length; i++) {
				try {
					Map<String,Object> map_one = new HashMap<String,Object>();
					map_one.put("id", hits[i].getId());
					for(String field:fields){
						map_one.put(field, hits[i].getFields().get(field).getValue());
					}
					list_res.add(map_one);
					logger.debug(map_one.toString());
				} catch (Exception e) {
					logger.error("get hist result error:"+e.getMessage());
					continue ; // 可能单条数据存在数据异常，则忽略跳过
				}
			}
		}
		
		return list_res;
		
	}

	/**
	 * 关闭该连接
	 */
	public void close(){
		
		client.close();
		
	}
	
}