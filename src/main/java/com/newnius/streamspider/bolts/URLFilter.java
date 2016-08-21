package com.newnius.streamspider.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.SpiderConfig;
import com.newnius.streamspider.model.UrlPatternFactory;
import com.newnius.streamspider.model.UrlPatternSetting;
import com.newnius.streamspider.util.JedisDAO;
import com.newnius.streamspider.util.StringConverter;

import redis.clients.jedis.Jedis;

public class URLFilter implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8252575652969522973L;
	private Logger logger;
	private OutputCollector collector;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple tuple) {
		String url = tuple.getStringByField("url");
		Jedis jedis = JedisDAO.getInstance();
		String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
		if (pattern != null) {/* in allowed url_patterns */
			UrlPatternSetting patternSetting = UrlPatternFactory.getPatternSetting(pattern);
			int expireTime = patternSetting.getFrequency();
			long count = StringConverter.string2int(jedis.get("url_pattern_download_count_" + pattern), 0);
			if (count < patternSetting.getLimitation() || patternSetting.getLimitation() == -1) {
				String res = jedis.set("up_to_date_" + url, url, "NX", "EX", expireTime);
				if (res != null) { // this url is not up_to_date or never downloaded
					collector.emit("filtered-url", new Values(url));
					logger.info("emit filtered url " + url);
					jedis.incr("url_pattern_download_count_" + pattern);
				}
				if (count == 0) {// set expire time if not set
					jedis.expire("url_pattern_download_count_" + pattern, SpiderConfig.LIMITATION_RESET_INTERVAL);
				}
			}
		}
		jedis.close();
		collector.ack(tuple);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.logger = LoggerFactory.getLogger(getClass());
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclearer) {
		outputDeclearer.declareStream("filtered-url", new Fields("url"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
