package com.newnius.streamspider.bolts;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.model.UrlPatternFactory;
import com.newnius.streamspider.model.UrlPatternSetting;
import com.newnius.streamspider.util.StringConverter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

public class URLSaver implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8575035865296521244L;
	private OutputCollector collector;
	private Jedis jedis;
	private Logger logger;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.jedis = new Jedis(stormConf.get("host").toString(), new Integer(stormConf.get("port").toString()));
		this.logger = LoggerFactory.getLogger(getClass());
	}

	@Override
	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		Set<String> urls = (Set<String>) input.getValueByField("urls");

		for (String url : urls) {
			String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
			if (pattern != null && !jedis.exists("up_to_date_" + url)) {
				UrlPatternSetting patternSetting = UrlPatternFactory.getPatternSetting(pattern);
				int count = StringConverter.string2int(jedis.get("url_pattern_download_count_" + pattern), 0);
				if (count < patternSetting.getLimitation() || patternSetting.getLimitation() == -1) {
					jedis.lpush("urls_to_download", url);
					logger.info("push url " + url);
				}
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
