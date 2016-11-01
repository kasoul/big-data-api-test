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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
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
	private String scrollId;

	/**
	 * constructor
	 * 
	 * @param ipAddress String,ip地址或主机名
	 * @param clusterName String,集群名称
	 * @param indexName String,索引名称
	 * @param typeName String,类型名称
	 */
	public ESSearcher(String ipAddress, String clusterName,String indexName,String typeName){
		
		this(ipAddress, clusterName, 9300, indexName, typeName);
		
	}
	
	/**
	 * constructor
	 * 
	 * @param ipAddress String,ip地址或主机名
	 * @param clusterName String,集群名称
	 * @param transportPort int,端口号
	 * @param indexName String,索引名称
	 * @param typeName String,类型名称
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
	 * @param id String,文档id
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
	 * @param ids Iterable<String>,文档id集合
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
	 * 最大返回1000000条记录
	 * @param fields String..., 需要返回的字段
	 * @return List<Map<String, Object>>,查询结果，文档集合
	 */
	public List<Map<String, Object>> scan(String... fields) {

		return scan(0,1000000,fields);

	}
	
	
	/**
	 * 扫描一个索引的所有文档
	 * @param from int, 起始索引
	 * @param size int, 每页数量
	 * @param fields String..., 需要返回的字段
	 * @return List<Map<String, Object>>,查询结果，文档集合
	 */
	public List<Map<String, Object>> scan(int from, int size,String... fields) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				.setFrom(from)
				.setSize(size)
				.addFields(fields)
				.execute()
				.actionGet();
		SearchHit[] hits = searchResponse.getHits().getHits();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>() ;
		if(hits !=null && hits.length > 0){
			for (int i = 0; i < hits.length; i++) {
				try {
					Map<String,Object> map_one = new HashMap<String,Object>();
					map_one.put("id", hits[i].getId());
					
					for(String field:fields){
						SearchHitField searchHitField = hits[i].getFields().get(field);
						map_one.put(field, searchHitField.value());
					}
					
					list.add(map_one);
					logger.debug(map_one.toString());
				} catch (Exception e) {
					logger.error("get hits result error:" + e.getMessage(),e);
					continue ; 
				}
			}
		}
		
		return list;
		
	}
	
	/**
	 * 扫描一个索引的所有文档,只取一个字段，字段类型为array
	 * @param field String, 需要返回的字段
	 * @return Map<String, Object>, 查询结果，文档集合
	 */
	public Map<String, Object> scanSingleArrayField(int from, int size, String field) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				.setFrom(from)
				.setSize(size)
				.addFields(field)
				.execute()
				.actionGet();
		
		SearchHit[] hits = searchResponse.getHits().getHits();
		Map<String,Object> map = new HashMap<String,Object>() ;

		if(hits !=null && hits.length > 0){
			for (int i = 0; i < hits.length; i++) {
				try {
					SearchHitField searchHitField = hits[i].getFields().get(field);
					map.put(hits[i].getId(), searchHitField.values());
					logger.debug(hits[i].getId());
				} catch (Exception e) {
					logger.error("get hits result error:" + e.getMessage(),e);
					continue ; 
				}
			}
		}
		
		return map;
		
	}
	
	/**
	 * 扫描一个索引的所有文档,字段类型为array
	 * 最大返回1000000条记录
	 * @param fields String..., 需要返回的字段
	 * @return List<Map<String, Object>>, 查询结果，文档集合
	 */
	public List<Map<String, Object>> scanArrayField(String... fields) {

		return scanArrayField(0, 500000, fields);
		
	}
	
	/**
	 * 扫描一个索引的所有文档,字段类型为array
	 * @param from int, 起始索引
	 * @param size int, 每页数量
	 * @param fields String..., 需要返回的字段
	 * @return List<Map<String, Object>>,查询结果，文档集合
	 */
	public List<Map<String, Object>> scanArrayField(int from, int size,String... fields) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				.setFrom(from)
				.setSize(size)
				.addFields(fields)
				.execute()
				.actionGet();
		SearchHit[] hits = searchResponse.getHits().getHits();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>() ;
		if(hits !=null && hits.length > 0){
			for (int i = 0; i < hits.length; i++) {
				try {
					Map<String,Object> map_one = new HashMap<String,Object>();
					map_one.put("id", hits[i].getId());
					
					for(String field:fields){
						SearchHitField searchHitField = hits[i].getFields().get(field);
						map_one.put(field, searchHitField.values());
					}
					
					list.add(map_one);
					logger.debug(map_one.toString());
				} catch (Exception e) {
					logger.error("get hits result error:" + e.getMessage(),e);
					continue ; // 可能单条数据存在数据异常，则忽略跳过
				}
			}
		}
		
		return list;
		
	}
	
	/**
	 * 滚动查询扫描一个索引的所有文档
	 * @param fields String..., 需要返回的字段
	 * @return List<Map<String, Object>>, 查询结果，文档集合
	 */
	public List<Map<String, Object>> scrollScan(String... fields) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				.setSearchType(SearchType.SCAN)
				.setScroll(TimeValue.timeValueMinutes(8))
				.setSize(5)
				.addFields(fields)
				.execute()
				.actionGet();
		
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>() ;
		
		while(true){
			
			SearchHit[] hits = searchResponse.getHits().getHits();
		
			if(hits !=null && hits.length > 0){
				for (int i = 0; i < hits.length; i++) {
					try {
						Map<String,Object> map_one = new HashMap<String,Object>();
						map_one.put("id", hits[i].getId());
					
						for(String field:fields){
							SearchHitField searchHitField = hits[i].getFields().get(field);
							map_one.put(field, searchHitField.value());
						}
						logger.info(hits[i].getId());
						list.add(map_one);
						logger.debug(map_one.toString());
					} catch (Exception e) {
						logger.error("get hits result error:" + e.getMessage(),e);
						continue ; // 可能单条数据存在数据异常，则忽略跳过
					}
				}
				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(TimeValue.timeValueMinutes(8))
						.execute()
						.actionGet();
			}else{
				break;
			}
		}

		return list;
		
	}
	
	/**
	 * 滚动查询扫描一个索引的所有文档,字段类型为array
	 * @param fields String..., 需要返回的字段
	 * @return List<Map<String, Object>>, 查询结果，文档集合
	 */
	public List<Map<String, Object>> scrollScanArrayField(String... fields) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				.setSearchType(SearchType.QUERY_AND_FETCH)
				.setScroll(TimeValue.timeValueMinutes(8))
				.setSize(1000)
				.addFields(fields)
				.execute()
				.actionGet();
		
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>() ;
		
		while(true){
			
			SearchHit[] hits = searchResponse.getHits().getHits();
		
			if(hits !=null && hits.length > 0){
				for (int i = 0; i < hits.length; i++) {
					try {
						Map<String,Object> map_one = new HashMap<String,Object>();
						map_one.put("id", hits[i].getId());
					
						for(String field:fields){
							SearchHitField searchHitField = hits[i].getFields().get(field);
							map_one.put(field, searchHitField.values());
						}
						//logger.info(hits[i].getId());
						list.add(map_one);
						logger.debug(map_one.toString());
					} catch (Exception e) {
						logger.error("get hits result error:" + e.getMessage(),e);
						continue ; // 可能单条数据存在数据异常，则忽略跳过
					}
				}
				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(TimeValue.timeValueMinutes(8))
						.execute()
						.actionGet();
			}else{
				break;
			}
		}

		return list;
		
	}
	
	/**
	 * 滚动查询扫描一个索引的部分文档,只取一个字段，字段类型为array
	 * @param size int, 本查询限定条数，最好是游标size的倍数
	 * @param field String, 需要返回的字段
	 * @return Map<String, Object>, 查询结果，文档集合
	 */
	public Map<String, Object> scrollScanSingleArrayField(int size,String field) {
		
		Map<String,Object> map = new HashMap<String,Object>();
		SearchResponse searchResponse = null;
		int count = 0;
		while(count<size){
			searchResponse = client.prepareSearchScroll(scrollId)
					.setScroll(TimeValue.timeValueMinutes(60))
					.execute()
					.actionGet();
			SearchHit[] hits = searchResponse.getHits().getHits();

			if(hits !=null && hits.length > 0){
				for (int i = 0; i < hits.length; i++) {
					try {
						SearchHitField searchHitField = hits[i].getFields().get(field);
						map.put(hits[i].getId(), searchHitField.values());
						logger.debug(hits[i].getId());
					} catch (Exception e) {
						logger.error("get hits result error:" + e.getMessage(),e);
						continue ; // 可能单条数据存在数据异常，则忽略跳过
					}
				}
				count += hits.length;
			}else{
				break;
			}
		}

		return map;
		
	}
	
	/**
	 * 设置一个初始游标,并返回第一次结果集合
	 * @param pageSize int, 每次返回条数
	 * @param field String, 需要返回的字段
	 * @return String, 游标
	 */
	public Map<String, Object> setScrollId(int pageSize,String field) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				//.setSearchType()
				.setScroll(TimeValue.timeValueMinutes(8))
				.setSize(pageSize)
				.addFields(field)
				.execute()
				.actionGet();
		this.scrollId = searchResponse.getScrollId();
		
		Map<String,Object> map = new HashMap<String,Object>();
		SearchHit[] hits = searchResponse.getHits().getHits();

		if(hits !=null && hits.length > 0){
			for (int i = 0; i < hits.length; i++) {
				try {
					SearchHitField searchHitField = hits[i].getFields().get(field);
					map.put(hits[i].getId(), searchHitField.values());
					logger.debug(hits[i].getId());
				} catch (Exception e) {
					logger.error("get hits result error:" + e.getMessage(),e);
					continue ; 
				}
			}
		}
		
		return map;
		
	}
	
	/**
	 * 滚动查询扫描一个索引的所有文档,只取一个字段，字段类型为array
	 * @param field String, 需要返回的字段
	 * @return Map<String, Object>, 查询结果，文档集合
	 */
	public Map<String, Object> scrollScanSingleArrayField(String field) {

		SearchResponse searchResponse = client.prepareSearch(indexName)
				//.setSearchType()
				.setScroll(TimeValue.timeValueMinutes(8))
				.setSize(1000)
				.addFields(field)
				.execute()
				.actionGet();
		
		Map<String,Object> map = new HashMap<String,Object>();
		
		while(true){
			
			SearchHit[] hits = searchResponse.getHits().getHits();

			if(hits !=null && hits.length > 0){
				for (int i = 0; i < hits.length; i++) {
					try {
						SearchHitField searchHitField = hits[i].getFields().get(field);
						map.put(hits[i].getId(), searchHitField.values());
						logger.debug(hits[i].getId());
					} catch (Exception e) {
						logger.error("get hits result error:" + e.getMessage(),e);
						continue ; 
					}
				}
				searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(TimeValue.timeValueMinutes(8))
						.execute()
						.actionGet();
			}else{
				break;
			}
		}

		return map;
		
	}
	
	/**
	 * 统计索引总数量
	 * @return long,总数量
	 */
	public int count() {

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName);
		searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);
		searchRequestBuilder.setSize(0);
		SearchResponse searchResponse = searchRequestBuilder
				.execute()
				.actionGet();
		long count = searchResponse.getHits().getTotalHits();
		
		return (int)count;
		
	}

	/**
	 * 关闭该连接
	 */
	public void close(){
		
		client.close();
		
	}
	
}