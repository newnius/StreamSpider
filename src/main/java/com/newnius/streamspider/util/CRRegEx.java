package com.newnius.streamspider.util;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * find certain string(s)
 * or validate string of email, phone etc.
 * 
 * @author Newnius
 * @version 0.1.0(General) 
 * Dependencies
 *  com.newnius.util.CRLogger
 *
 */
public class CRRegEx {
    private static final String TAG= "CRRegEx";
    private static CRLogger logger = CRLogger.getLogger(TAG);

    public static List<CRObject> findAll(String str, String pattern){
        List<CRObject> list = new ArrayList<>();
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(str);
        while (m.find()) {
            logger.debug("new match found:" + m.group());
            CRObject crObject = new CRObject();
            for(int i=0;i<=m.groupCount();i++){
            	if(m.group(i)!=null){
            		crObject.set(i + "", m.group(i));
            	}
            }
            list.add(crObject);
        }
        return list;
    }

    public static CRObject find(String str, String pattern){
        CRObject crObject = new CRObject();
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(str);
        if (m.find()) {
            logger.debug("new match found:" + m.group());
            for(int i=0;i<=m.groupCount();i++){
                crObject.set(i + "", m.group(i));
            }
            return crObject;
        }else{
            return crObject;
        }
    }

}
