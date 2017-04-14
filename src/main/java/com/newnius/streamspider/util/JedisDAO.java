package com.newnius.streamspider.util;

import redis.clients.jedis.Jedis;

public class JedisDAO {
	private Jedis jedis;
	private static String host;
	private static int port;

	public static void configure(CRObject config){
		host = config.get("REDIS_HOST");
		port = config.getInt("REDIS_PORT");
	}

	public static Jedis instance() {
		return new Jedis(host, port);
	}

	/*
	private static JedisPool pool;
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(32);
		config.setMaxIdle(10);
		config.setMinIdle(0);
		//config.setMaxWaitMillis(30000);
		config.setMinEvictableIdleTimeMillis(300000);
		config.setSoftMinEvictableIdleTimeMillis(-1);
		config.setNumTestsPerEvictionRun(3);
		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		config.setTestWhileIdle(false);
		config.setTimeBetweenEvictionRunsMillis(60000);// 一分钟
		// config.setBlockWhenExhausted(true);
	*/

}
