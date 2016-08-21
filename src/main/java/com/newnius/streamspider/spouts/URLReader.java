package com.newnius.streamspider.spouts;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.util.JedisDAO;

import redis.clients.jedis.Jedis;

public class URLReader implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2771580553730894069L;

	private SpoutOutputCollector collector;
	private Logger logger;
	private long freezeTime = 0;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(URLReader.class);
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
		if (System.currentTimeMillis() < freezeTime) {
			try {
				Thread.sleep(50);
				return;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		Jedis jedis = JedisDAO.getInstance();
		String url = jedis.rpop("urls_to_download");
		if (url != null) {
			logger.info("emit " + url);
			collector.emit("url", new Values(url));
		} else {
			try {
				// logger.info("no more url, wait.");
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		jedis.close();
	}

	@Override
	public void ack(Object msgId) {
		logger.info("ack " + (String) msgId);
	}

	@Override
	public void fail(Object msgId) {
		Jedis jedis = JedisDAO.getInstance();
		jedis.lpush("urls_to_download", (String) msgId);
		logger.info("fail " + (String) msgId);
		freezeTime = System.currentTimeMillis() + 300;
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
