package com.newnius.streamspider.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.*;
import java.util.List;
import java.util.Map;

/**
 * 
 * get or post to url to get resources many customized options
 * 
 * @author Newnius
 * @version 0.1.0(General) Dependencies: com.newnius.util.CRLogger
 *          com.newnius.util.CRMsg com.newnius.util.CRErrorCode
 */
public class CRSpider {
	private static final String TAG = "CRSpider";
	private CRLogger logger;
	private static int mode = CRSpider.PRODUCE_MODE;
	public static final int DEVELOP_MODE = 0;
	public static final int PRODUCE_MODE = 1;

	/*
	 *
	 * more mime-type can be found in
	 * http://www.iana.org/assignments/media-types/media-types.xhtml
	 *
	 */
	public static final String CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
	public static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";

	/*
	 * content-type(optional)
	 */
	private String contentType = CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED;

	/*
	 * user-agent(optional) more user-agent can be found in
	 * http://www.useragentstring.com
	 */
	private String userAgent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201";

	/*
	 * cookie(optional) will automatically set
	 */
	private String cookie = null;

	/*
	 * site url (required)
	 *
	 */
	private String urlStr;

	/*
	 * response content of given url
	 */
	private String sourceCode = null;

	/*
	 *
	 * target url encoding(optional)
	 *
	 */
	private String encoding = "UTF-8";

	/*
	 * whether follow 301/302 redirects(optional)
	 *
	 */
	private boolean followRedirects = true;

	/*
	 * response headers of given url
	 *
	 */
	private Map<String, List<String>> headers;

	/*
	*  optional
	* */
	private Proxy proxy;

	public CRSpider(String urlStr) {
		this.urlStr = urlStr;
		logger = CRLogger.getLogger(TAG);
	}

	public CRSpider setEncoding(String encoding) {
		this.encoding = encoding;
		return this;
	}

	public CRSpider setUrl(String urlStr) {
		this.urlStr = urlStr;
		return this;
	}

	public CRSpider setCookie(String cookie) {
		this.cookie = cookie;
		return this;
	}

	public CRSpider setFollowRedirects(boolean followRedirects) {
		this.followRedirects = followRedirects;
		return this;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getHeaderField(String key) {
		if (headers.containsKey(key)) {
			return headers.get(key).get(0);
		} else {
			return null;
		}
	}

	public void setProxy(Proxy proxy) {
		this.proxy = proxy;
	}

	public CRMsg doGet() {
		try {
			URL url = new URL(urlStr);
			HttpURLConnection conn;
			if(proxy!=null) {
				conn = (HttpURLConnection) url.openConnection(proxy);
			}else {
				conn = (HttpURLConnection) url.openConnection();
			}
			conn.setDoInput(true);
			conn.setDoOutput(true);
			conn.setRequestMethod("GET");

			if (cookie != null)
				conn.setRequestProperty("Cookie", cookie);
			conn.setRequestProperty("User-Agent", userAgent);
			conn.connect();

			InputStream is = conn.getInputStream();

			BufferedReader br = new BufferedReader(new InputStreamReader(is, encoding));
			String response = "";
			String readLine;
			while ((readLine = br.readLine()) != null) {
				response = response + readLine;
			}
			if (conn.getHeaderField("Set-Cookie") != null) {
				cookie = conn.getHeaderField("Set-Cookie");
			}
			headers = conn.getHeaderFields();

			is.close();
			br.close();
			conn.disconnect();
			sourceCode = response;
			CRMsg msg = new CRMsg(CRErrorCode.SUCCESS);
			msg.set("response", sourceCode);
			return msg;
		} catch (Exception ex) {
			if (CRSpider.mode == CRSpider.DEVELOP_MODE) {
				logger.error(ex);
			}
			return new CRMsg(CRErrorCode.FAIL, ex.getMessage());
		}
	}

	public CRMsg doPost(final Map<String, String> paramMap) {
		contentType = CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED;
		String postdata = "";
		try {
			for (Map.Entry<String, String> entry : paramMap.entrySet()) {
				postdata += entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), "utf-8") + "&";
			}
		} catch (Exception ex) {
			if (CRSpider.mode == CRSpider.DEVELOP_MODE) {
				logger.error(ex);
			}
			return new CRMsg(CRErrorCode.FAIL, ex.getMessage());
		}
		return doPost(postdata);
	}

	public CRMsg doPost(final String postdata) {
		try {
			URL url = new URL(urlStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoInput(true);
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			if (cookie != null)
				conn.setRequestProperty("Cookie", cookie);
			conn.setRequestProperty("User-Agent", userAgent);
			conn.setRequestProperty("Content-Type", contentType);
			conn.setInstanceFollowRedirects(followRedirects);
			conn.connect();
			DataOutputStream out = new DataOutputStream(conn.getOutputStream());
			out.writeBytes(postdata);
			out.flush();
			out.close();
			InputStream is = conn.getInputStream();

			BufferedReader br = new BufferedReader(new InputStreamReader(is, encoding));
			String response = "";
			String readLine;
			while ((readLine = br.readLine()) != null) {
				response = response + readLine;
			}

			if (conn.getHeaderField("Set-Cookie") != null) {
				cookie = conn.getHeaderField("Set-Cookie");
			}
			headers = conn.getHeaderFields();

			is.close();
			br.close();
			conn.disconnect();
			sourceCode = response;
			CRMsg msg = new CRMsg(CRErrorCode.SUCCESS);
			msg.set("response", sourceCode);
			return msg;
		} catch (Exception ex) {
			if (CRSpider.mode == CRSpider.DEVELOP_MODE) {
				logger.error(ex);
			}
			return new CRMsg(CRErrorCode.FAIL, ex.getMessage());
		}
	}

}
