package com.newnius.streamspider.bolts;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.model.UrlPatternFactory;

public class HTMLParser implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5766304044396284108L;
	private OutputCollector collector;
	private Logger logger;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(getClass());
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getStringByField("url");
		String html = input.getStringByField("html");

		/* get all links as possible urls */
		Document doc = Jsoup.parse(html);
		Elements links = doc.select("a");
		List<String> possibleUrls = new ArrayList<>();
		for (Element link : links) {
			possibleUrls.add(link.attr("href"));
		}
		
		String relatedPattern = UrlPatternFactory.getRelatedUrlPattern(url);
		if (relatedPattern != null) {
			Set<String> urls = new TreeSet<>();	
			List<String> patterns = UrlPatternFactory.getPatternSetting(relatedPattern).getPatterns2follow();
			for (String pattern : patterns) {
				logger.info("use pattern " + pattern);
				for (String possibleUrl : possibleUrls) {
					try {
						if (possibleUrl.contains("(")) {
							continue;// ignore javascript:foo(bar)
						}
						String absoluteUrl = new URL(new URL(url), possibleUrl).toString();
						if (absoluteUrl.matches(pattern)) {
							urls.add(absoluteUrl);
							logger.info("new url " + absoluteUrl);
						}
					} catch (MalformedURLException e) {
						e.printStackTrace();
					}
				}
			}
			collector.emit("urls", new Values(urls));
		}
		collector.ack(input);
	}

	
	/* patterns 4 test */
	private List<String> getPatterns() {
		List<String> patterns = new ArrayList<>();
		patterns.add("http://blog.csdn.net/u014663710/article/details/\\d+");
		patterns.add("http://blog.csdn.net/u014663710/article/category/\\d+(/\\d+)?");
		patterns.add("http://blog.csdn.net/u014663710/article/list/\\d+");
		patterns.add("http://blog.csdn.net/u014663710");
		return patterns;
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("urls", new Fields("urls"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
