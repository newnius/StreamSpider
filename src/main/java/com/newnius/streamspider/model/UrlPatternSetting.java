package com.newnius.streamspider.model;


public class UrlPatternSetting {
	private int expire;
	private int limitation;
	private int interval;
	private int parallelism;
	
	public UrlPatternSetting(int expire, int limitation, int interval, int parallelism) {
		this.expire = expire;
		this.limitation = limitation;
		this.interval = interval;
		this.parallelism = parallelism;
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

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
}