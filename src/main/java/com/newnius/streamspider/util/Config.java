package com.newnius.streamspider.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by newnius on 4/17/17.
 *
 */
public class Config {
    private static Map<String, String> map = new HashMap<>();


    public static void put(String key, String value){
        map.put(key, value);
    }


    public static String get(String key){
        return get(key, null);
    }

    public static String get(String key, String defaultVal){
        if(!map.containsKey(key)){
            return defaultVal;
        }
        return map.get(key);
    }

}
