package com.newnius.streamspider.spouts;

import java.util.Map;

import com.newnius.streamspider.util.CRObject;
import com.newnius.streamspider.util.JedisDAO;
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

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(URLReader.class);
		CRObject config = new CRObject();
		config.set("REDIS_HOST", conf.get("REDIS_HOST").toString());
		config.set("REDIS_PORT", Integer.parseInt(conf.get("REDIS_PORT").toString()));
		JedisDAO.configure(config);
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
		String url = null;
		try {
			Jedis jedis = JedisDAO.instance();
			url = jedis.rpop("urls_to_download");
			jedis.close();
		}catch (Exception ex){
			ex.printStackTrace();
		}
		if (url != null) {
			logger.debug("emit " + url);
			collector.emit("url", new Values(url));
		} else {
			try {
				// logger.info("no more url, wait.");
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void ack(Object msgId) {
		logger.info("ack " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		try {
			Jedis jedis = JedisDAO.instance();
			jedis.lpush("urls_to_download", (String) msgId);
			jedis.close();
			logger.warn("fail " + msgId);
		}catch (Exception ex){
			ex.printStackTrace();
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