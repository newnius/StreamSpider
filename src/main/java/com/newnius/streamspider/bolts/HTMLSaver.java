package com.newnius.streamspider.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.newnius.streamspider.SpiderConfig;
import com.newnius.streamspider.model.UrlPatternFactory;
import com.newnius.streamspider.util.JedisDAO;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

public class HTMLSaver implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7736795729454993219L;
	private OutputCollector collector;
	private MongoDatabase mongoDatabase;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient(SpiderConfig.mongodb_host, SpiderConfig.mongodb_port);
			// 连接到数据库
			this.mongoDatabase = mongoClient.getDatabase("StreamSpider");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		String html = input.getStringByField("html");
		/* store html into mongodb */
		MongoCollection<Document> collection = mongoDatabase.getCollection("pages");
		// 插入文档
		/**
		 * 1. 创建文档 org.bson.Document 参数为key-value的格式 2. 创建文档集合List<Document> 3.
		 * 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>) 插入单个文档可以用
		 * mongoCollection.insertOne(Document)
		 */
		Document document = new Document("url", url).append("html", html);
		List<Document> documents = new ArrayList<Document>();
		documents.add(document);
		collection.insertMany(documents);

		/* update redis */
		Jedis jedis = JedisDAO.getInstance();
		//jedis.set("last_update_" + url, "");
		String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
		int expireTime = pattern == null ? SpiderConfig.DEFAULT_FREQUENCY
				: UrlPatternFactory.getPatternSetting(pattern).getFrequency();
		jedis.expire("last_update_" + url, expireTime);
		collector.ack(input);
		jedis.close();
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
