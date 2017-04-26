package com.newnius.streamspider.bolts;

import java.net.URL;
import java.util.Map;

import com.newnius.streamspider.model.UrlPatternSetting;
import com.newnius.streamspider.util.CRObject;
import com.newnius.streamspider.util.StringConverter;
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
			int delay = (int)input.getValueByField("delay")*1000;
			String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
			if (pattern != null && !jedis.exists("up_to_date_" + url)) {
				if(jedis.zscore("urls_to_download", url)==null) {
					UrlPatternSetting patternSetting = UrlPatternFactory.getPatternSetting(pattern);
					String host = new URL(url).getHost();
					long count = StringConverter.string2int(jedis.get("countq_" + host), 0);
					if (count < patternSetting.getLimitation()+50 || patternSetting.getLimitation() == -1) {
						jedis.zadd("urls_to_download", System.currentTimeMillis()+delay, url);
                        jedis.incr("countq_" + host);
						logger.debug("push url " + url);
					}
				}
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
