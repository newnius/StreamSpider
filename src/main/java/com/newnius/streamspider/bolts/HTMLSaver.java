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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTMLSaver implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7736795729454993219L;
	private OutputCollector collector;
	private Logger logger;
	private String QUEUE_NAME;
    private ConnectionFactory factory;
	private Channel channel;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(getClass());
		QUEUE_NAME = conf.get("MQ_QUEUE").toString();
		String MQ_HOST = conf.get("MQ_HOST").toString();
		factory = new ConnectionFactory();
		factory.setHost(MQ_HOST);
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			String url = tuple.getStringByField("url");
			String html = tuple.getStringByField("html");
			String charset = tuple.getStringByField("charset");
			if(html.length() > 200000){
				collector.ack(tuple);
				return;
			}
			if(channel==null){
				Connection connection = factory.newConnection();
				channel = connection.createChannel();
				channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			}
			MQMessage msg = new MQMessage(url, html, System.currentTimeMillis(), charset);
			String message = new Gson().toJson(msg);
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
		}catch (Exception ex){
			channel=null;
			logger.warn(ex.getClass().getSimpleName()+":"+ex.getMessage());
		}
		collector.ack(tuple);
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
