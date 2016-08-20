package com.newnius.streamspider.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.util.CRErrorCode;
import com.newnius.streamspider.util.CRMsg;
import com.newnius.streamspider.util.CRSpider;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Downloader implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7624774326486651896L;
	private OutputCollector collector;
	private Logger logger;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(Downloader.class);
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");

		CRSpider spider = new CRSpider(url);
		CRMsg msg = spider.doGet();
		if (msg.getCode() == CRErrorCode.SUCCESS) {
			String html = msg.get("response");
			logger.info("Downloaded " + url);
			collector.emit("html", new Values(url, html));
		} else {
			logger.info(msg.getMessage());
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("html", new Fields("url", "html"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
