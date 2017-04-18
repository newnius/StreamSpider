package com.newnius.streamspider.bolts;

import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.newnius.streamspider.SpiderConfig;

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

		Document document = new Document("url", url);
		Document newDocument = new Document("url", url).append("html", html).append("version",
				System.currentTimeMillis());
		FindIterable<Document> ite = collection.find(document).limit(1);
		if (ite.first() == null) {
			collection.insertOne(newDocument);
		} else {
			collection.replaceOne(ite.first(), newDocument);
		}
		collector.ack(input);




		try {
			String QUEUE_NAME = "hello";
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			String message = "Hello World!";
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + message + "'");
			channel.close();
			connection.close();
		}catch (Exception ex){
			ex.printStackTrace();
		}





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
