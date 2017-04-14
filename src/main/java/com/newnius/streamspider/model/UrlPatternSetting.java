package com.newnius.streamspider.model;


public class UrlPatternSetting {
	private String pattern;
	private int expire;
	private int limitation;
	private int interval = 5 * 60;
	
	public UrlPatternSetting(String pattern, int expire, int limitation) {
		this.pattern = pattern;
		this.expire = expire;
		this.limitation = limitation;
	}

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public int getExpire() {
		return expire;
	}

	public void setExpire(int expire) {
		this.expire = expire;
	}

	public int getLimitation() {
		return limitation;
	}

	public void setLimitation(int limitation) {
		this.limitation = limitation;
	}

	public int getInterval() {
		return interval;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}
}
