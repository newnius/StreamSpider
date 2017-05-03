package com.newnius.streamspider.util;

import java.net.URL;
import java.nio.charset.Charset;

/**
 * Created by newnius on 4/28/17.
 *
 */
public class StringUtils {


    /*
    *
    * handle urls with white space and Chinese characters
    * From: http://www.jianshu.com/p/9be694c8fee2
    * */
    public static String encodeUrl(URL url, Charset charset){
        String query = url.getQuery();
        if(query !=null) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < query.length(); i++) {
                char c = query.charAt(i);
                if (c <= 255) {
                    sb.append(c);
                } else {
                    byte[] b;
                    try {
                        b = String.valueOf(c).getBytes(charset);
                    } catch (Exception ex) {
                        b = new byte[0];
                    }
                    for (byte aB : b) {
                        int k = aB;
                        if (k < 0)
                            k += 256;
                        sb.append("%").append(Integer.toHexString(k).toUpperCase());
                    }
                }
            }
            query = sb.toString();
        }

        String res = url.getProtocol()+"://";
        res+=url.getHost();
        if(url.getPort()!=-1){
            res+=":"+url.getPort();
        }
        if(url.getPath()!=null){
            res+=url.getPath();
        }
        if(query != null){
            res+= "?"+query;
        }
        //fix url encode bug
        res = res.replaceAll(" ", "%20");
        return res;
    }
}
