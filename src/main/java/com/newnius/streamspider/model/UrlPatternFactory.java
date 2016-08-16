package com.newnius.streamspider.model;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.SpiderConfig;
import com.newnius.streamspider.util.StringConverter;

import redis.clients.jedis.Jedis;

public class UrlPatternFactory {
	private static Set<String> patterns = null;
	private static long lastUpdate = 0;
	private static Jedis jedis = null;
	private static Logger logger = LoggerFactory.getLogger(UrlPatternFactory.class);

	public static Set<String> getAllPatterns() {
		if (System.currentTimeMillis() - lastUpdate > SpiderConfig.PATTERNS_CACHE_MILLISECOND || patterns == null) {
			fetchAllPatterns();
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
		logger.info("getPatternSetting url_pattern_setting_" + pattern);
		Map<String, String> pairs = jedis.hgetAll("url_pattern_setting_" + pattern);

		for (Entry<String, String> entry : pairs.entrySet()) {
			logger.info("Pattern setting: " + entry.getKey() + "=>" + entry.getValue());
		}

		int frequency = StringConverter.string2int(pairs.get("frequency"), SpiderConfig.DEFAULT_FREQUENCY);
		int limitation = StringConverter.string2int(pairs.get("limitation"), SpiderConfig.DEFAULT_LIMITATION);
		String patterns2followStr = pairs.get("patterns2follow");
		if (patterns2followStr == null) {
			patterns2followStr = SpiderConfig.PATTERN_URL;
		}

		String[] tmp = patterns2followStr.split(",");
		List<String> patterns2follow = Arrays.asList(tmp);
		UrlPatternSetting patternSetting = new UrlPatternSetting(pattern, frequency, limitation,patterns2follow);
		return patternSetting;
	}

	private static void fetchAllPatterns() {
		if (jedis == null) {
			jedis = new Jedis(SpiderConfig.redis_host, SpiderConfig.redis_port);
		}
		patterns = jedis.zrange("allowed_url_patterns", 0, -1);
		for (String pattern : patterns) {
			logger.info("Pattern " + pattern);
		}
	}

}
