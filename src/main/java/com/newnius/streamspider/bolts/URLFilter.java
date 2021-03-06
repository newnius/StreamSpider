package com.newnius.streamspider.bolts;

import java.net.URL;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.newnius.streamspider.util.CRObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newnius.streamspider.model.UrlPatternFactory;
import com.newnius.streamspider.model.UrlPatternSetting;
import com.newnius.streamspider.util.JedisDAO;
import com.newnius.streamspider.util.StringConverter;

import redis.clients.jedis.Jedis;

public class URLFilter implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8252575652969522973L;
	private Logger logger;
	private OutputCollector collector;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");
        String pattern = UrlPatternFactory.getRelatedUrlPattern(url);
        if(isFile(url) || pattern==null){
            logger.debug(url+" is not web page or not in white list.");
            collector.ack(tuple);
            return;
        }
        try (Jedis jedis = JedisDAO.instance()) {
            UrlPatternSetting patternSetting = UrlPatternFactory.getPatternSetting(pattern);
            int expireTime = patternSetting.getExpire();
            String host = new URL(url).getHost();
            long count = StringConverter.string2int(jedis.get("count_" + host), 0);
            if (count < patternSetting.getLimitation() || patternSetting.getLimitation() == -1) {
                int lastUpdate = StringConverter.string2int(jedis.get("up_to_date_" + url), 0);
                // this url is not up_to_date or never downloaded
                if(lastUpdate==0 || (patternSetting.getExpire()!=-1 && (System.currentTimeMillis()/1000-lastUpdate)>expireTime)){
                    long time = System.currentTimeMillis()/1000;
                    jedis.set("up_to_date_" + url, time+"");
                    int no = ThreadLocalRandom.current().nextInt(0, patternSetting.getParallelism());
                    collector.emit("filtered-url", tuple, new Values(host + "." + no, url));
                    logger.debug("emit filtered url " + url);
                    jedis.incr("count_" + host);
                    if (count < 5) {// set expire time if not set, == 0 may not take effect in high concurrency
                        jedis.expire("count_" + host, patternSetting.getInterval());
                    }
                }else{
                    logger.debug(url+" already up to date "+lastUpdate);
                }
            } else {
                collector.emit("url", new Values(url));
                logger.debug("go back "+url);
            }
        } catch (Exception ex) {
            logger.warn(ex.getClass().getSimpleName()+":"+ex.getMessage());
        }
        collector.ack(tuple);
    }

    private boolean isFile(String url){
        String[] array = {
                ".png", ".jpg", ".jpeg", ".gif", ".bmp",
                ".flv", ".swf", ".mkv", ".avi", ".rm", ".rmvb", ".mpeg", ".mpg",
                ".ogg", ".ogv", ".mov", ".wmv", ".mp4", ".webm", ".mp3", ".wav", ".mid",
                ".rar", ".zip", ".tar", ".gz", ".7z", ".bz2", ".cab", ".iso",
                ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".pdf", ".txt", ".md", ".xml"
        };
        Set<String> extensions = new HashSet<>(Arrays.asList(array));
        String extension = "";
        int i = url.lastIndexOf('.');
        if (i > 0) {
            extension = url.substring(i).toLowerCase();
        }
        return extensions.contains(extension);
    }

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.logger = LoggerFactory.getLogger(getClass());
		this.collector = collector;
		CRObject config = new CRObject();
		config.set("REDIS_HOST", conf.get("REDIS_HOST").toString());
		config.set("REDIS_PORT", Integer.parseInt(conf.get("REDIS_PORT").toString()));
		JedisDAO.configure(config);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputDeclarer) {
		outputDeclarer.declareStream("filtered-url", new Fields("pattern", "url"));
		outputDeclarer.declareStream("url", new Fields("url"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
