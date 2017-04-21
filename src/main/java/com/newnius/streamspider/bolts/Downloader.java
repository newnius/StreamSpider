package com.newnius.streamspider.bolts;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Map;

import com.newnius.streamspider.util.StringConverter;
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
	private String proxy_host;
	private int proxy_port;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(Downloader.class);
		proxy_host = conf.get("PROXY_HOST").toString();
		proxy_port = StringConverter.string2int(conf.get("PROXY_PORT").toString(), 7001);
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		CRSpider spider = new CRSpider(url);
        Proxy proxy = null;
        if(proxy_host!=null) {
            proxy = new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxy_host, proxy_port));
        }
        spider.setProxy(proxy);
		CRMsg msg = spider.doGet();
		if (msg.getCode() == CRErrorCode.SUCCESS) {
			String html = msg.get("response");
			logger.debug("Downloaded: " + url);
			collector.emit("html", new Values(url, html));
		} else {
			logger.warn(msg.getMessage()+"("+url+")");
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
