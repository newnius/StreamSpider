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
import redis.clients.jedis.Tuple;

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
			Set<Tuple> tuples = jedis.zrangeWithScores("urls_to_download", 0, 0); // [start, stop]
            for(Tuple tuple: tuples) {
                if(tuple.getScore() > System.currentTimeMillis()){
                    Thread.sleep(50);
                    continue;
                }
                jedis.zrem("urls_to_download", tuple.getElement());
                logger.debug("emit " + tuple.getElement());
                collector.emit("url", new Values(tuple.getElement()), tuple.getElement());
				String host = new URL(tuple.getElement()).getHost();
				jedis.incrBy("countq_"+host, -1);
            }
            if(tuples.size()==0){
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
        try (Jedis jedis = JedisDAO.instance()) {
            long cnt = jedis.sadd("failed_urls", (String)msgId);
            if(cnt==1) {//first fail
                //reset flag
                jedis.del("up_to_date_"+msgId);
                //re-emit
                collector.emit("url", new Values((String) msgId), msgId);
            }//else ignore
            //logger.warn("fail "+msgId);
        }catch (Exception ex){
            logger.warn(ex.getMessage());
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