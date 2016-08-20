package com.newnius.streamspider.bolts;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.model.UrlPatternFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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

		Document doc = Jsoup.parse(html);
		Elements links = doc.select("a");
		List<String> possibleUrls = new ArrayList<>();
		for (Element link : links) {
			String possibleUrl = link.attr("href");
			possibleUrls.add(possibleUrl);
		}

		Set<String> urls = new TreeSet<>();

		String relatedPattern = UrlPatternFactory.getRelatedUrlPattern(url);
		if (relatedPattern != null) {
			List<String> patterns = UrlPatternFactory.getPatternSetting(relatedPattern).getPatterns2follow();
			for (String pattern : patterns) {
				logger.info("use pattern " + pattern);
				for (String possibleUrl : possibleUrls) {
					try {
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
		}
		collector.emit("urls", new Values(urls));
		collector.ack(input);
	}

	private List<String> getPatterns() {
		List<String> patterns = new ArrayList<>();
		//
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
