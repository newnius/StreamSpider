package com.newnius.streamspider.model;

import java.util.List;

public class UrlPatternSetting {
	private String pattern;
	private int frequency;
	private int limitation;
	private List<String> patterns2follow;
	
	public UrlPatternSetting(String pattern, int frequency, int limitation, List<String> patterns2follow) {
		super();
		this.pattern = pattern;
		this.frequency = frequency;
		this.limitation = limitation;
		this.patterns2follow = patterns2follow;
	}
	public String getPattern() {
		return pattern;
	}
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}
	public int getFrequency() {
		return frequency;
	}
	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	public List<String> getPatterns2follow() {
		return patterns2follow;
	}
	public void setPatterns2follow(List<String> patterns2follow) {
		this.patterns2follow = patterns2follow;
	}
	public int getLimitation() {
		return limitation;
	}
	public void setLimitation(int limitation) {
		this.limitation = limitation;
	}
	
	
	
	
	
	
}
