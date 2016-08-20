package com.newnius.streamspider;

import com.newnius.streamspider.bolts.Downloader;
import com.newnius.streamspider.bolts.HTMLParser;
import com.newnius.streamspider.bolts.HTMLSaver;
import com.newnius.streamspider.bolts.URLSaver;
import com.newnius.streamspider.spouts.URLReader;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class SpiderTopology {
	public static void main(String[] args) {
		Config conf = new Config();
		conf.put("host", "192.168.56.110");
		conf.put("port", "6379");

		SpiderConfig.redis_host = "192.168.56.110";
		SpiderConfig.redis_port = 6379;

		int url_reader_parallelism = 1;
		int downloader_parallelism = 8;
		int html_saver_parallelism = 1;
		int html_parser_parallelism = 2;
		int url_saver_parallelism = 1;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("url-reader", new URLReader(), url_reader_parallelism);

		builder.setBolt("downloader", new Downloader(), downloader_parallelism).shuffleGrouping("url-reader", "url");

		builder.setBolt("html-parser", new HTMLParser(), html_parser_parallelism).shuffleGrouping("downloader", "html");

		builder.setBolt("html-saver", new HTMLSaver(), html_saver_parallelism).shuffleGrouping("downloader", "html");

		builder.setBolt("url-saver", new URLSaver(), url_saver_parallelism).shuffleGrouping("html-parser", "urls");

		String topologyName = SpiderConfig.TOPOLOGY_NAME;

		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
