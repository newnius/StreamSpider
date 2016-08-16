package com.newnius.streamspider.util;

public class StringConverter {
	public static Long string2long(String s) {
		try {
			return Long.parseLong(s);
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	public static Long string2long(String s, long defaultValue) {
		Long longValue = string2long(s);
		return longValue != null ? longValue : defaultValue;
	}
	
	public static Integer string2int(String s) {
		try {
			return Integer.parseInt(s);
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	public static Integer string2int(String s, int defaultValue) {
		Integer intValue = string2int(s);
		return intValue != null ? intValue : defaultValue;
	}


}
