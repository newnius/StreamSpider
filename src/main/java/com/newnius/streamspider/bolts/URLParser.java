package com.newnius.streamspider.bolts;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

import com.newnius.streamspider.util.CRObject;
import com.newnius.streamspider.util.JedisDAO;
import com.newnius.streamspider.util.StringUtils;
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

public class URLParser implements IRichBolt {

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
	public void execute(Tuple tuple) {
		String url = tuple.getStringByField("url");
		String html = tuple.getStringByField("html");
		String charset = tuple.getStringByField("charset");

		/* get all links as possible urls */
		Document doc = Jsoup.parse(html);
		Elements links = doc.select("a");
		for (Element link : links) {
			String possibleUrl = link.attr("href");
			try {
				URL absoluteUrl = new URL(new URL(url), possibleUrl);
				if(!absoluteUrl.getProtocol().equals("http") && !absoluteUrl.getProtocol().equals("https")){
					continue;
				}
				String newUrl = absoluteUrl.getProtocol() + "://" + absoluteUrl.getHost();
				if (absoluteUrl.getPort() != -1) {
					newUrl += ":" + absoluteUrl.getPort();
				}
				newUrl += absoluteUrl.getFile();
				newUrl = StringUtils.encodeUrl(new URL(newUrl), Charset.forName(charset));
				collector.emit("url", new Values(newUrl));// no anchor to speed up
				logger.debug("new url " + newUrl);
			} catch (MalformedURLException ignored) {
			}
		}
		collector.ack(tuple);
	}

	@Override
	public void cleanup() {

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
