package com.newnius.streamspider;

import com.newnius.streamspider.bolts.*;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import com.newnius.streamspider.spouts.URLReader;
import org.apache.storm.tuple.Fields;

public class SpiderTopology {
	public static void main(String[] args) {
		Config conf = new Config();
		conf.put("REDIS_HOST", "ss-redis");
		conf.put("REDIS_PORT", "6379");
		conf.put("MQ_HOST", "ss-rabbitmq");
		conf.put("MQ_QUEUE", "ss-pages");
		conf.put("MQ_QUEUE_URLS", "ss-urls");
		//conf.put("PROXY_HOST","ss-proxy");
		//conf.put("PROXY_PORT","7001");

		conf.setMaxSpoutPending(10000);
		conf.setNumWorkers(4);
		conf.setMessageTimeoutSecs(60);

		int url_reader_parallelism = 1;
		int url_filter_parallelism = 3;
		int downloader_parallelism = 100;
		int html_saver_parallelism = 1;
		int html_parser_parallelism = 1;
		int url_saver_parallelism = 5;


		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("url-reader", new URLReader(), url_reader_parallelism);

		builder.setBolt("url-filter", new URLFilter(), url_filter_parallelism).shuffleGrouping("url-reader", "url");

		builder.setBolt("downloader", new Downloader(), downloader_parallelism).fieldsGrouping("url-filter", "filtered-url", new Fields("pattern"));

		builder.setBolt("url-parser", new URLParser(), html_parser_parallelism).shuffleGrouping("downloader", "html");

		builder.setBolt("html-saver", new HTMLSaver(), html_saver_parallelism).shuffleGrouping("downloader", "html");

		builder.setBolt("url-saver", new URLSaver(), url_saver_parallelism).shuffleGrouping("url-parser", "url").shuffleGrouping("url-filter", "url");

		String topologyName = "StreamSpider";

		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
