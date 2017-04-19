package com.newnius.streamspider.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.SpiderConfig;
import com.newnius.streamspider.util.JedisDAO;
import com.newnius.streamspider.util.StringConverter;

import redis.clients.jedis.Jedis;

public class UrlPatternFactory {
	private static Set<String> patterns = null;
	private static Map<String, UrlPatternSetting> settings = new HashMap<>();
	private static long lastUpdate = 0;
	private static Logger logger = LoggerFactory.getLogger(UrlPatternFactory.class);

	public static Set<String> getAllPatterns() {
		if (System.currentTimeMillis() - lastUpdate > SpiderConfig.PATTERNS_CACHE_MILLISECOND || patterns == null) {
			fetchAllPatterns();
			settings = new HashMap<>();
			lastUpdate = System.currentTimeMillis();
		}
		return patterns;
	}

	public static String getRelatedUrlPattern(String url) {
		getAllPatterns();
		for (String pattern : patterns) {
			if (url.matches(pattern)) {
				return pattern;
			}
		}
		return null;
	}


	public static UrlPatternSetting getPatternSetting(String pattern) {
		if(settings.containsKey(pattern)){
			return settings.get(pattern);
		}
		logger.debug("Re-fetching pattern setting " + pattern);
		try (Jedis jedis = JedisDAO.instance()) {
			Map<String, String> pairs = jedis.hgetAll("url_pattern_setting_" + pattern);
			for (Entry<String, String> entry : pairs.entrySet()) {
				logger.debug("Pattern setting: " + entry.getKey() + "=>" + entry.getValue());
			}
			int expire = StringConverter.string2int(pairs.get("expire"), SpiderConfig.DEFAULT_EXPIRE_SECOND);
			int limitation = StringConverter.string2int(pairs.get("limitation"), SpiderConfig.DEFAULT_LIMITATION);
			int interval = StringConverter.string2int(pairs.get("interval"), SpiderConfig.DEFAULT_INTERVAL);
			int parallelism = StringConverter.string2int(pairs.get("parallelism"), SpiderConfig.DAFAULT_PARALLELISM);
			UrlPatternSetting patternSetting = new UrlPatternSetting(expire, limitation, interval, parallelism);
			settings.put(pattern, patternSetting);
			return patternSetting;
		} catch (Exception ex) {
			logger.warn(ex.getMessage());
			return new UrlPatternSetting(1,1,1,1);
		}
	}

	private static void fetchAllPatterns() {
		Jedis jedis = JedisDAO.instance();
		patterns = jedis.zrevrangeByScore("allowed_url_patterns", SpiderConfig.PRIORITY_HIGHEST, SpiderConfig.PRIORITY_LOWEST);
		for (String pattern : patterns) {
			logger.debug("Load pattern " + pattern);
		}
		jedis.close();
	}

}
