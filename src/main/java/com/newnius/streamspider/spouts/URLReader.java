package com.newnius.streamspider.spouts;

import java.net.URL;
import java.util.Map;
import java.util.Set;

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
		try (Jedis jedis = JedisDAO.instance()) {
			Set<String> urls = jedis.zrange("urls_to_download", 0, 0); // [start, stop]
            for(String url: urls) {
                jedis.zrem("urls_to_download", url);
                logger.debug("emit " + url);
                collector.emit("url", new Values(url), url);

				String host = new URL(url).getHost();
				jedis.incrBy("countq_"+host, -1);
            }
            if(urls.size()==0){
				logger.debug("no more url, wait.");
				Thread.sleep(100);
            }
		} catch (Exception ex) {
			logger.warn(ex.getMessage());
		}

	}

	@Override
	public void ack(Object msgId) {
		logger.debug("ack " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		try {
			Jedis jedis = JedisDAO.instance();
			jedis.zadd("urls_to_download", System.currentTimeMillis(), (String) msgId);
			jedis.close();
			logger.warn("fail " + msgId);
		}catch (Exception ex){
			logger.error(ex.getMessage());
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