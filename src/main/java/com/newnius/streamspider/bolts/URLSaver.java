package com.newnius.streamspider.bolts;

import java.util.Map;
import java.util.Set;

import com.newnius.streamspider.util.CRObject;
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
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(getClass());
		CRObject config = new CRObject();
		config.set("REDIS_HOST", conf.get("REDIS_HOST").toString());
		config.set("REDIS_PORT", Integer.parseInt(conf.get("REDIS_PORT").toString()));
		JedisDAO.configure(config);
	}

	@Override
	public void execute(Tuple input) {
		try (Jedis jedis = JedisDAO.instance()) {
			String url = (String) input.getValueByField("url");
			String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
			if (pattern != null && !jedis.exists("up_to_date_" + url)) {
				jedis.lpush("urls_to_download", url);
				logger.debug("push url " + url);
			}
		} catch (Exception ex) {
			logger.warn(ex.getMessage());
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
