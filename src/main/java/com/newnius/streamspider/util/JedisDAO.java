package com.newnius.streamspider.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisDAO {
	private static String host;
	private static int port;

	private static JedisPool pool;

	public static void configure(CRObject config){
		host = config.get("REDIS_HOST");
		port = config.getInt("REDIS_PORT");

		JedisPoolConfig jconfig = new JedisPoolConfig();
		jconfig.setMaxTotal(32);
		jconfig.setMaxIdle(10);
		jconfig.setMinIdle(0);
		//jconfig.setMaxWaitMillis(30000);
		jconfig.setMinEvictableIdleTimeMillis(300000);
		jconfig.setSoftMinEvictableIdleTimeMillis(-1);
		jconfig.setNumTestsPerEvictionRun(3);
		jconfig.setTestOnBorrow(false);
		jconfig.setTestOnReturn(false);
		jconfig.setTestWhileIdle(false);
		jconfig.setTimeBetweenEvictionRunsMillis(60000);// 一分钟
		// jconfig.setBlockWhenExhausted(true);
		pool = new JedisPool(jconfig, host, port);
	}

	public static Jedis instance() {
		//return new Jedis(host, port);
		return pool.getResource();
	}
}
