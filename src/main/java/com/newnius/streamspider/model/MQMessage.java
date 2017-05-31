package com.newnius.streamspider.model;

/**
 * Created by newnius on 4/19/17.
 *
 */
public class MQMessage {
    private String url;
    private String html;
    private long time;
    private String charset;

    public MQMessage(String title, String html, long time, String charset) {
        this.url = title;
        this.html = html;
        this.time = time;
        this.charset = charset;
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

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }
}
