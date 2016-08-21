package com.newnius.streamspider.bolts;

import java.util.Map;
import java.util.Set;

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

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(getClass());
	}

	@Override
	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		Set<String> urls = (Set<String>) input.getValueByField("urls");
		Jedis jedis = JedisDAO.getInstance();
		for (String url : urls) {
			String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
			if (pattern != null) {
				jedis.lpush("urls_to_download", url);
				logger.info("push url " + url);
			}
		}
		jedis.close();
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
