package com.newnius.streamspider.bolts;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

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
        try (Jedis jedis = JedisDAO.instance()) {
            String url = tuple.getStringByField("url");
            String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
            if (pattern == null) {
                collector.ack(tuple);
                return;
            }
            UrlPatternSetting patternSetting = UrlPatternFactory.getPatternSetting(pattern);
            int expireTime = patternSetting.getExpire();
            String host = new URL(url).getHost();
            long count = StringConverter.string2int(jedis.get("count_" + host), 0);
            if (count < patternSetting.getLimitation() || patternSetting.getLimitation() == -1) {
                String res = jedis.set("up_to_date_" + url, "1", "NX", "EX", expireTime);
                if (res != null) { // this url is not up_to_date or never downloaded
                    int no = ThreadLocalRandom.current().nextInt(0, patternSetting.getParallelism());
                    collector.emit("filtered-url", new Values(pattern + "." + no, url));
                    logger.debug("emit filtered url " + url);
                    jedis.incr("count_" + host);
                }
                if (count == 0) {// set expire time if not set
                    jedis.expire("count_" + host, patternSetting.getInterval());
                }
            } else {
                if (!jedis.exists("up_to_date_" + url)) {
                    collector.emit("url", new Values(url));
                }
            }
            collector.ack(tuple);
        } catch (Exception ex) {
            ex.printStackTrace();
            collector.fail(tuple);
        }
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
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		outputDeclarer.declareStream("filtered-url", new Fields("pattern", "url"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
