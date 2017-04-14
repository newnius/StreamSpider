package com.newnius.streamspider;

public class SpiderConfig {
	public static final String TOPOLOGY_NAME = "StreamSpider";

	// public static String mongodb_host = "192.168.56.110";

	public static String mongodb_host = "172.18.0.8";

	public static int mongodb_port = 27017;

	public static final int DEFAULT_PAGE_CACHE_SECOND = 2 * 60;

	public static final long PATTERNS_CACHE_MILLISECOND = 60 * 1000;

	public static final int DEFAULT_INTERVAL = 5 * 60;

	public static final int DEFAULT_LIMITATION = -1;

	public static final int DEFAULT_FREQUENCY = 10 * 60;// ;1 * 60 * 60;

	public static final int PRIORITY_LOWEST = 1;

	public static final int PRIORITY_HIGHEST = 5;

}
