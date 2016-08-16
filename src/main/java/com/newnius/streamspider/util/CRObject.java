package com.newnius.streamspider.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * present an object without creating a new class note: unable to present list
 * now
 * 
 * @author Newnius
 * @version 0.1.0(General) Dependencies com.newnius.util.CRLogger
 */
public class CRObject {
	private HashMap<String, String> data;
	private HashMap<String, CRObject> objects;
	private HashMap<String, List<CRObject>> lists;

	public CRObject() {
		data = new HashMap<>();
		objects = new HashMap<>();
		lists = new HashMap<>();
	}

	public void unset(String key) {
		if (data.containsKey(key)) {
			data.remove(key);
		} else {
			CRLogger.warn(getClass().getName(), key + " not exist.");
		}
	}

	public void set(String key, String value) {
		if (data.containsKey(key)) {
			data.remove(key);
			CRLogger.info(getClass().getName(), key + " is overwritten.");
		}
		data.put(key, value);
	}

	public void set(String key, Integer value) {
		if (data.containsKey(key)) {
			data.remove(key);
			CRLogger.info(getClass().getName(), key + " is overwritten.");
		}
		data.put(key, value + "");
	}

	public void set(String key, Float value) {
		if (data.containsKey(key)) {
			data.remove(key);
			CRLogger.info(getClass().getName(), key + " is overwritten.");
		}
		data.put(key, value + "");
	}

	public void set(String key, Boolean value) {
		if (data.containsKey(key)) {
			data.remove(key);
			CRLogger.info(getClass().getName(), key + " is overwritten.");
		}
		// 0 for false, others for true
		data.put(key, !value ? "0" : "1");
	}

	public void set(String key, CRObject object) {
		if (objects.containsKey(key)) {
			objects.remove(key);
			CRLogger.info(getClass().getName(), key + " is overwritten.");
		}
		objects.put(key, object);
	}

	public String get(String key) {
		if (data.containsKey(key))
			return data.get(key);
		CRLogger.warn(getClass().getName(), key + " not exist.");
		return null;
	}

	public Integer getInt(String key) {
		try {
			if (data.containsKey(key))
				return Integer.parseInt(data.get(key));
			CRLogger.warn(getClass().getName(), key + " not exist.");
			return null;
		} catch (Exception ex) {
			return null;
		}
	}

	public Long getLong(String key) {
		try {
			if (data.containsKey(key))
				return Long.parseLong(data.get(key));
			CRLogger.warn(getClass().getName(), key + " not exist.");
			return null;
		} catch (Exception ex) {
			return null;
		}
	}

	public Float getFloat(String key) {
		try {
			if (data.containsKey(key))
				return Float.parseFloat(data.get(key));
			CRLogger.warn(getClass().getName(), key + " not exist.");
			return null;
		} catch (Exception ex) {
			return null;
		}
	}

	public Boolean getBoolean(String key) {
		try {
			if (data.containsKey(key))
				return data.get(key).equals("0") ? false : true;
			CRLogger.warn(getClass().getName(), key + " not exist.");
			return null;
		} catch (Exception ex) {
			return null;
		}
	}

	public CRObject getObject(String key) {
		if (objects.containsKey(key))
			return objects.get(key);
		CRLogger.warn(getClass().getName(), key + " not exist.");
		return null;
	}

	public boolean hasKey(String key) {
		return data.containsKey(key);
	}

	@Override
	public String toString() {
		String str = "CRObject\n{\n";
		for (Map.Entry<String, String> entry : data.entrySet()) {
			str += "    " + entry.getKey() + "==>" + entry.getValue() + "\n";
		}
		str += "},\n";
		str += "objects==>{\n";
		for (Map.Entry<String, CRObject> entry : objects.entrySet()) {
			str += "    " + entry.getKey() + "==>" + entry.getValue().toString() + "\n";
		}
		str += "},\n";
		str += "lists==>{\n";
		for (Map.Entry<String, List<CRObject>> entry : lists.entrySet()) {
			str += entry.getKey() + "==>\n";
			for (CRObject object : (List<CRObject>) entry.getValue()) {
				str += "    " + object.toString() + "\n";
			}

		}
		str += "}";
		return str;
	}

	public void set(String key, List<CRObject> list) {
		setList(key, list);
	}

	public void setList(String key, List<CRObject> list) {
		if (lists.containsKey(key)) {
			lists.remove(key);
			CRLogger.info(getClass().getName(), key + " is overwritten.");
		}
		lists.put(key, list);
	}

	public List<CRObject> getList(String key) {
		if (lists.containsKey(key))
			return lists.get(key);
		CRLogger.warn(getClass().getName(), key + " not exist.");
		return null;
	}
}
