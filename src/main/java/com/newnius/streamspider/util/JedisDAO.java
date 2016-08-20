package com.newnius.streamspider.util;

import com.newnius.streamspider.SpiderConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisDAO {
	private static JedisPool pool;
	static {
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
		pool = new JedisPool(config, SpiderConfig.redis_host, SpiderConfig.redis_port);
	}

	public static Jedis getInstance() {
		Jedis client = pool.getResource();// 从pool中获取资源
		return client;
	}

}
