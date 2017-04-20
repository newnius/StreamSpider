package com.newnius.streamspider.bolts;

import java.util.Map;

import com.google.gson.Gson;
import com.newnius.streamspider.model.MQMessage;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HTMLSaver implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7736795729454993219L;
	private OutputCollector collector;
	private String QUEUE_NAME;
    private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		QUEUE_NAME = conf.get("MQ_QUEUE").toString();
		factory = new ConnectionFactory();
		factory.setHost(conf.get("MQ_HOST").toString());
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		}catch (Exception ex){
			ex.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		String html = input.getStringByField("html");
		try {
			MQMessage msg = new MQMessage(url, html, System.currentTimeMillis());
			String message = new Gson().toJson(msg);
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
		}catch (Exception ex){
			ex.printStackTrace();
			try{
				channel.close();
				connection.close();
                connection = factory.newConnection();
                channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			}catch (Exception ignore){
			}
		}
		collector.ack(input);
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
