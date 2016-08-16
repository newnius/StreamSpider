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

		int spout_Parallelism_hint = 1;
		int message_divider_Parallelism_hint = 2;
		int order_container_Parallelism_hint = 3;
		int count_Parallelism_hint = 1;
		int tair_write_Parallelism_hint = 2;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("url-reader", new URLReader(), spout_Parallelism_hint);

		builder.setBolt("downloader", new Downloader(), message_divider_Parallelism_hint).shuffleGrouping("url-reader",
				"url");

		builder.setBolt("html-parser", new HTMLParser(), order_container_Parallelism_hint).shuffleGrouping("downloader",
				"html");

		builder.setBolt("html-saver", new HTMLSaver(), count_Parallelism_hint).shuffleGrouping("downloader", "html");

		builder.setBolt("url-saver", new URLSaver(), tair_write_Parallelism_hint).shuffleGrouping("html-parser",
				"urls");

		String topologyName = SpiderConfig.TOPOLOGY_NAME;

		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
