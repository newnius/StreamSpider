package com.newnius.streamspider;

import com.newnius.streamspider.util.JedisDAO;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class SpiderConfig {
    private static Map<String, String> settings = new HashMap<>();
    private static long lastUpdate = 0;

	public static final int DEFAULT_EXPIRE_SECOND = 7 * 24 * 60 * 60; // 7 day

	public static final long PATTERNS_CACHE_MILLISECOND = 60 * 1000;

	public static final int DEFAULT_INTERVAL = 5 * 60;

	public static final int DEFAULT_LIMITATION = 2 * DEFAULT_INTERVAL;

	public static final int DAFAULT_PARALLELISM = 1;

	public static final int PRIORITY_LOWEST = 1;

	public static final int PRIORITY_HIGHEST = 5;

	public static String get(String key){
        return get(key, null);
    }

    public static String get(String key, String defaultVal){
        if (System.currentTimeMillis() - lastUpdate > SpiderConfig.PATTERNS_CACHE_MILLISECOND) {
            loadSettings();
            lastUpdate = System.currentTimeMillis();
        }
        if(settings.containsKey(key))
            return settings.get(key);
        return defaultVal;
    }

    private static void loadSettings(){
        settings = new HashMap<>();
        Jedis jedis = JedisDAO.instance();
        Map<String, String> pairs = jedis.hgetAll("settings");
        for (Map.Entry<String, String> entry : pairs.entrySet()) {
            settings.put(entry.getKey(), entry.getValue());
        }
    }

}
