package com.newnius.streamspider.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * log message and save them
 * 
 * @author Newnius
 * @version 0.1.0(java SE version)
 * 
 * dependencies
 *  log4j-api-2.x.jar
 *  log4j-core-2.x.jar
 *  configure file log4j2.xml in your class path
 *  
 *  source code in git@github.com:newnius/util-java.git
 *
 */
public class CRLogger {
    private final String TAG;
    private Logger logger = null;


    /*
    * @param clazz
    * */
    private CRLogger(String clazz) {
        this.TAG = clazz;
        this.logger = LoggerFactory.getLogger(TAG);
    }


    public static <T> CRLogger getLogger(Class<T> clazz) {
        return new CRLogger(clazz.getName());
    }

    public static CRLogger getLogger(String clazz) {
        return new CRLogger(clazz);
    }

    public static void debug(String tag, String msg) {
        getLogger(tag).debug(msg);
    }

    public void debug(String msg) {
    	logger.debug(msg);
    }

    public static void info(String tag, String msg) {
        getLogger(tag).info(msg);
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public static void warn(String tag, String msg) {
        getLogger(tag).warn(msg);
    }

    public void warn(String msg) {
        logger.warn(msg);
    }

    public static void error(String tag, String msg) {
        getLogger(tag).error(msg);
    }

    public void error(String msg) {
        logger.error(msg);
    }

    public static void error(String tag, Exception ex) {
        getLogger(tag).error(ex);
    }

    public void error(Exception ex) {
        logger.error(ex.getMessage(), ex);
    }

}
