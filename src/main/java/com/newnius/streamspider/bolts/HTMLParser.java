package com.newnius.streamspider.bolts;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.newnius.streamspider.util.CRObject;
import com.newnius.streamspider.util.JedisDAO;
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
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.logger = LoggerFactory.getLogger(getClass());
		CRObject config = new CRObject();
		config.set("REDIS_HOST", conf.get("REDIS_HOST").toString());
		config.set("REDIS_PORT", Integer.parseInt(conf.get("REDIS_PORT").toString()));
		JedisDAO.configure(config);
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
			Set<String> patterns = UrlPatternFactory.getAllPatterns();
			for (String pattern : patterns) {
				for (String possibleUrl : possibleUrls) {
					try {
						URL absoluteUrl = new URL(new URL(url), possibleUrl);
						String newUrl = absoluteUrl.getProtocol()+"://"+absoluteUrl.getHost();
						if(absoluteUrl.getPort() != -1){
							newUrl += ":"+absoluteUrl.getPort();
						}
						newUrl += absoluteUrl.getFile();
						if (newUrl.matches(pattern)) {
							urls.add(newUrl);
							logger.info("new url " + newUrl);
						}
					} catch (MalformedURLException e) {
						//e.printStackTrace();
					}
				}
			}
			collector.emit("urls", new Values(urls));
		}
		collector.ack(input);
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
