package com.newnius.streamspider;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import com.newnius.streamspider.bolts.Downloader;
import com.newnius.streamspider.bolts.HTMLParser;
import com.newnius.streamspider.bolts.HTMLSaver;
import com.newnius.streamspider.bolts.URLFilter;
import com.newnius.streamspider.bolts.URLSaver;
import com.newnius.streamspider.spouts.URLReader;

public class SpiderTopology {
	public static void main(String[] args) {
		Config conf = new Config();

		int url_reader_parallelism = 1;
		int url_filter_parallelism = 1;
		int downloader_parallelism = 5;
		int html_saver_parallelism = 1;
		int html_parser_parallelism = 1;
		int url_saver_parallelism = 1;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("url-reader", new URLReader(), url_reader_parallelism);

		builder.setBolt("url-filter", new URLFilter(), url_filter_parallelism).shuffleGrouping("url-reader", "url");

		builder.setBolt("downloader", new Downloader(), downloader_parallelism).shuffleGrouping("url-filter",
				"filtered-url");

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
