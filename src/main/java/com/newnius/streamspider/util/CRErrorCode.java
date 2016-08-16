package com.newnius.streamspider.util;

/**
 * 
 * use errorcode instead of using int directly
 * 
 * @author Newnius
 * @version 0.1.0(General)
 */
public class CRErrorCode extends CRCode{
    public static final int SUCCESS = 0;
    public static final int FAIL = 1;
    public static final int INVALID_FORMAT = 2;
    public static final int UNKNOWN_REQUEST_CODE = 3;

    
    public static String getMsg(int errorCode){
        switch (errorCode){
            case SUCCESS:
                return "成功";
            default:
                return "出现错误 (错误码:"+errorCode+")";
        }
    }
    
    


}
