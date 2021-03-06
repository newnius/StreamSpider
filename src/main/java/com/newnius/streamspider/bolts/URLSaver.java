package com.newnius.streamspider.bolts;

import java.net.URL;
import java.util.Map;

import com.newnius.streamspider.model.UrlPatternSetting;
import com.newnius.streamspider.util.CRObject;
import com.newnius.streamspider.util.StringConverter;
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

import com.newnius.streamspider.model.UrlPatternFactory;
import com.newnius.streamspider.util.JedisDAO;

import redis.clients.jedis.Jedis;

public class URLSaver implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8575035865296521244L;
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
		CRObject config = new CRObject();
		config.set("REDIS_HOST", conf.get("REDIS_HOST").toString());
		config.set("REDIS_PORT", Integer.parseInt(conf.get("REDIS_PORT").toString()));
		JedisDAO.configure(config);

		QUEUE_NAME = conf.get("MQ_QUEUE_URLS").toString();
		String MQ_HOST = conf.get("MQ_HOST").toString();
		factory = new ConnectionFactory();
		factory.setHost(MQ_HOST);

	}

	@Override
	public void execute(Tuple tuple) {
		try (Jedis jedis = JedisDAO.instance()) {
			if(channel==null){
				Connection connection = factory.newConnection();
				channel = connection.createChannel();
				channel.queueDeclare(QUEUE_NAME, false, true, false, null);
			}
			String url = (String) tuple.getValueByField("url");
			String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
			if (pattern != null) {
				UrlPatternSetting patternSetting = UrlPatternFactory.getPatternSetting(pattern);
				String host = new URL(url).getHost();
				long count = StringConverter.string2int(jedis.get("countq_" + host), 0);
				if (count < patternSetting.getLimitation()+50 || patternSetting.getLimitation() == -1) {
					channel.basicPublish("", QUEUE_NAME, null, url.getBytes("UTF-8"));
					jedis.incr("countq_" + host);
					logger.debug("push url " + url);
				}
			}
		} catch (Exception ex) {
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
