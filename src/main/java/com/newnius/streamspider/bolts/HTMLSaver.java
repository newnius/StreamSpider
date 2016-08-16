package com.newnius.streamspider.bolts;

import java.util.Map;

import com.newnius.streamspider.SpiderConfig;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

public class HTMLSaver implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7736795729454993219L;
	private OutputCollector collector;
	private Jedis jedis;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.jedis = new Jedis(stormConf.get("host").toString(), new Integer(stormConf.get("port").toString()));
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		String html = input.getStringByField("html");
		/* store html into mongodb */

		/* update redis */
		jedis.set("up_to_date_" + url, "");
		jedis.expire("up_to_date_" + url, SpiderConfig.DEFAULT_PAGE_CACHE_SECOND);
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
