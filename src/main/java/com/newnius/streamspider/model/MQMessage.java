package com.newnius.streamspider.model;

/**
 * Created by newnius on 4/19/17.
 *
 */
public class MQMessage {
    private String url;
    private String html;
    private long time;

    public MQMessage(String title, String html, long time) {
        this.url = title;
        this.html = html;
        this.time = time;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
