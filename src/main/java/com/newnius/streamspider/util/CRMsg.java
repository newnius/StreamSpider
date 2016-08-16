package com.newnius.streamspider.util;

/**
 * 
 * extends CRObject, often used in socket connection to exchange message
 * 
 * @author Newnius
 * @version 0.1.0(General) Dependencies com.newnius.util.CRObject
 *
 * 			@// FIXME: 16-4-27 unable to add list
 *
  
 *          */
public class CRMsg extends CRObject {
	private int code = 0;
	private String message = "";

	public CRMsg(int code) {
		this(code, "");
	}

	public CRMsg(int code, String message) {
		super();
		this.code = code;
		this.message = message;
	}

	public int getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	

}
