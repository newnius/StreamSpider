package com.newnius.streamspider;

public class SpiderConfig {
	public static final String TOPOLOGY_NAME = "StreamSpider";

	// public static String mongodb_host = "192.168.56.110";

	public static String mongodb_host = "172.18.0.8";

	public static int mongodb_port = 27017;

	public static final int DEFAULT_EXPIRE_SECOND = 24 * 60 * 60; // 1 day

	public static final long PATTERNS_CACHE_MILLISECOND = 60 * 1000;

	public static final int DEFAULT_INTERVAL = 5 * 60;

	public static final int DEFAULT_LIMITATION = 2 * DEFAULT_INTERVAL;

	public static final int DAFAULT_PARALLELISM = 1;

	public static final int PRIORITY_LOWEST = 1;

	public static final int PRIORITY_HIGHEST = 5;

}
