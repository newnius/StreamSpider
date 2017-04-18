package com.newnius.streamspider.bolts;

import java.net.InetSocketAddress;
import java.net.Proxy;
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

import com.newnius.streamspider.util.CRErrorCode;
import com.newnius.streamspider.util.CRMsg;
import com.newnius.streamspider.util.CRSpider;


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

		//Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("176.93.133.144", 8080));
		Proxy proxy = new Proxy(Proxy.Type.SOCKS, new InetSocketAddress("ss-proxy", 7001));
		spider.setProxy(proxy);

		CRMsg msg = spider.doGet();
		if (msg.getCode() == CRErrorCode.SUCCESS) {
			String html = msg.get("response");
			logger.info("Downloaded: " + url);
			collector.emit("html", new Values(url, html));
		} else {
			logger.warn(msg.getMessage());
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
