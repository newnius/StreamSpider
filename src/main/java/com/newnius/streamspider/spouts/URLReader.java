package com.newnius.streamspider.spouts;

import java.net.URL;
import java.util.Map;

import com.newnius.streamspider.util.CRObject;
import com.newnius.streamspider.util.JedisDAO;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class URLReader implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2771580553730894069L;

	private SpoutOutputCollector collector;
	private Logger logger;
	private String QUEUE_NAME;
	private ConnectionFactory factory;
	private Channel channel = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(URLReader.class);
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
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void nextTuple() {
		try (Jedis jedis = JedisDAO.instance()) {
			if(channel==null){
				Connection connection = factory.newConnection();
				channel = connection.createChannel();
				channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			}
			GetResponse response = channel.basicGet(QUEUE_NAME, true);//auto Ack
			if(response != null) {
				String url = new String(response.getBody());
				logger.debug("emit " + url);
				collector.emit("url", new Values(url), url);
				String host = new URL(url).getHost();
				jedis.incrBy("countq_" + host, -1);
			}else{
				String url = "http://www.tsinghua.edu.cn/publish/newthu/index.html";
				collector.emit("url", new Values(url), url);
				Thread.sleep(50);
				logger.info("No more urls, waiting...");
			}
		} catch (Exception ex) {
			channel = null;
			logger.warn(ex.getClass().getSimpleName()+":"+ex.getMessage());
		}
	}

	@Override
	public void ack(Object msgId) {
		logger.debug("ack " + msgId);
	}

	@Override
	public void fail(Object msgId) {
        try (Jedis jedis = JedisDAO.instance()) {
			//reset flag
			jedis.del("up_to_date_"+msgId);
			//re-emit
			collector.emit("url", new Values((String) msgId), msgId);
        }catch (Exception ex){
			logger.warn(ex.getClass().getSimpleName()+":"+ex.getMessage());
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("url", new Fields("url"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}