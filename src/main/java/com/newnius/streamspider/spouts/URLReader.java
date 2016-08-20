package com.newnius.streamspider.spouts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.SpiderConfig;
import com.newnius.streamspider.model.UrlPatternFactory;
import com.newnius.streamspider.util.JedisDAO;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
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
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		Jedis jedis = JedisDAO.getInstance();
		String url = jedis.rpop("urls_to_download");
		if (url != null) {
			logger.info("emit " + url);

			String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
			if (pattern != null) {
				long count = jedis.incr("url_pattern_download_count_" + pattern);
				if (count == 1) {
					jedis.expire("url_pattern_download_count_" + pattern,
							SpiderConfig.DEFAULT_LIMITATION_RESET_INTERVAL);
				}
			}

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
