package com.newnius.streamspider.bolts;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;
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

import com.newnius.streamspider.util.CRSpider;


public class Downloader implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7624774326486651896L;
	private OutputCollector collector;
	private Logger logger;
	private String proxy_host = null;
	private int proxy_port;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(Downloader.class);
		if(conf.containsKey("PROXY_HOST")) {
			proxy_host = conf.get("PROXY_HOST").toString();
		}
		if(conf.containsKey("PROXY_PORT")) {
			proxy_port = StringConverter.string2int(conf.get("PROXY_PORT").toString(), 7001);
		}
	}

	@Override
	public void execute(Tuple tuple) {
		String url = tuple.getStringByField("url");
		CRSpider spider = new CRSpider();

        List<String> mimes = new ArrayList<>();
        mimes.add("text/html");
        spider.setAllowedMimeTypes(mimes);
        if(proxy_host!=null) {
			spider.setProxy(Proxy.Type.SOCKS, proxy_host, proxy_port);
		}
		spider.doGet(url);
		if (spider.getStatusCode() == 200) {
			String html = spider.getHtml();
			String charset = spider.getCharset();
			logger.debug("Downloaded: " + url);
            if(html != null){// mime type not respected
                collector.emit("html", new Values(url, html, charset));
            }
		} else if(spider.getErrMsg() != null){
			logger.warn(spider.getErrMsg()+"("+url+")");
		}
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("html", new Fields("url", "html", "charset"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
