package com.newnius.streamspider.bolts;

import java.util.Map;

import com.newnius.streamspider.util.CRObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		Jedis jedis = JedisDAO.instance();
		String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
		if (pattern != null) {/* in allowed url_patterns */
			UrlPatternSetting patternSetting = UrlPatternFactory.getPatternSetting(pattern);
			int expireTime = patternSetting.getExpire();
			long count = StringConverter.string2int(jedis.get("url_pattern_download_count_" + pattern), 0);
			if (count < patternSetting.getLimitation() || patternSetting.getLimitation() == -1) {
				String res = jedis.set("up_to_date_" + url, "1", "NX", "EX", expireTime);
				if (res != null) { // this url is not up_to_date or never downloaded
					collector.emit("filtered-url", new Values(url));
					logger.info("emit filtered url " + url);
					jedis.incr("url_pattern_download_count_" + pattern);
				}
				if (count == 0) {// set expire time if not set
					jedis.expire("url_pattern_download_count_" + pattern, patternSetting.getInterval());
				}
			}else{
				jedis.lpush("urls_to_download", url);
			}
		}
		jedis.close();
		collector.ack(tuple);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.logger = LoggerFactory.getLogger(getClass());
		this.collector = collector;
		CRObject config = new CRObject();
		config.set("REDIS_HOST", conf.get("REDIS_HOST").toString());
		config.set("REDIS_PORT", Integer.parseInt(conf.get("REDIS_PORT").toString()));
		JedisDAO.configure(config);
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
