package com.newnius.streamspider.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * get or post to url to get resources many customized options
 *
 * @author Newnius
 * @version 0.1.0
 * 	Dependencies:
 * 		com.newnius.util.CRLogger
 * 		com.newnius.util.CRErrorCode
 */
public class CRSpider {
	private static final String TAG = "CRSpider";
	private CRLogger logger;

	/*
	 *
	 * more mime-type can be found in
	 * http://www.iana.org/assignments/media-types/media-types.xhtml
	 *
	 */
	public static final String CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
	public static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";

	/*
	 * content-type
	 */
	private String contentType = CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED;

	private String userAgent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201";

	private List<String> cookies;

	private String currentStr;

	private String charset = null;

	private boolean followRedirects = true;

	private List<String> allowedMimeTypes;

	private Proxy.Type proxy_type = Proxy.Type.DIRECT;

	private String proxy_host;

	private int proxy_port;

	private HttpClientContext context;

	private String html;

	private int statusCode;

	private String errMsg;

	public CRSpider() {
		logger = CRLogger.getLogger(TAG);
		userAgent = UA.random();
		cookies = new ArrayList<>();
		context = HttpClientContext.create();
		allowedMimeTypes = new ArrayList<>();
	}

	public CRSpider setCharset(String charset) {
		this.charset = charset;
		return this;
	}

	public CRSpider setAllowedMimeTypes(List<String> allowedMimeTypes){
		this.allowedMimeTypes = allowedMimeTypes;
		return this;
	}

	public CRSpider addCookie(String cookie) {
		this.cookies.add(cookie);
		return this;
	}

	public CRSpider setFollowRedirects(boolean followRedirects) {
		this.followRedirects = followRedirects;
		return this;
	}

	public CRSpider setContentType(String contentType) {
		this.contentType = contentType;
		return this;
	}

	public CRSpider setProxy(Proxy.Type type, String host, int port) {
		proxy_type = type;
		proxy_host = host;
		proxy_port = port;
		return this;
	}

	public String getHtml(){
		return html;
	}

	public int getStatusCode(){
		return statusCode;
	}

	public String getErrMsg() {
		return errMsg;
	}

	/*
	* doGet
	* auto fill cookie, referer, convert charset, choose UA
	* */
	public void doGet(String url) {
		try {
			url = encodeUrl(url);
			clean(url);
			HttpGet httpGet = new HttpGet();
			httpGet.setURI(new URI(url));
			httpGet.setHeader("User-Agent", userAgent);
			for(String cookie: cookies){// Add customized cookies
				httpGet.addHeader("Cookie", cookie);
			}
			if(currentStr!=null) {
				httpGet.addHeader("Referer", currentStr);
			}
			HttpClient httpClient = buildHttpClient();
			HttpResponse response = httpClient.execute(httpGet, context);
			statusCode = response.getStatusLine().getStatusCode();
			HttpEntity entity = response.getEntity();
			if(entity==null){
				errMsg = "Entity is null";
				return;
			}
			parseHeaders(entity);
			if (errMsg != null){
				return;
			}
			if(followRedirects && (statusCode==301 || statusCode ==302)){
				currentStr = url;
				url = response.getFirstHeader("Location").getValue();
				url = new URL(new URL(currentStr), url).toString();
				url = new String(url.getBytes(charset),"utf-8");
				url = encodeUrl(url);
				doGet(url);
				return;
			}
			if (statusCode != 200){
				errMsg = "Status Code is "+statusCode;
				return;
			}
			parseBody(entity);
			currentStr = url;
		}catch (Exception ex){
			errMsg = ex.getMessage();
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
			logger.error(ex.getMessage());
			return new CRMsg(CRErrorCode.FAIL, ex.getMessage());
		}
		return doPost(postdata);
	}


	/**
	 * Untested
	 */
	public CRMsg doPost(final String postdata) {
		try {
			URL url = new URL(currentStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoInput(true);
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			for(String cookie: cookies) {
				conn.addRequestProperty("Cookie", cookie);
			}
			conn.setRequestProperty("User-Agent", userAgent);
			conn.setRequestProperty("Content-Type", contentType);
			conn.setInstanceFollowRedirects(followRedirects);
			conn.connect();
			DataOutputStream out = new DataOutputStream(conn.getOutputStream());
			out.writeBytes(postdata);
			out.flush();
			out.close();
			InputStream is = conn.getInputStream();

			BufferedReader br = new BufferedReader(new InputStreamReader(is, charset));
			String response = "";
			String readLine;
			while ((readLine = br.readLine()) != null) {
				response = response + readLine;
			}
			//headers = conn.getHeaderFields();

			is.close();
			br.close();
			conn.disconnect();
			String sourceCode = response;
			CRMsg msg = new CRMsg(CRErrorCode.SUCCESS);
			msg.set("response", sourceCode);
			return msg;
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			return new CRMsg(CRErrorCode.FAIL, ex.getMessage());
		}
	}

	/*
	* clean cookie of anaother site
	* clean errMsg, status code
	* */
	private void clean(String url){
		statusCode = 0;
		errMsg = null;
		try {
			if (currentStr != null && !new URL(currentStr).getHost().equals(new URL(url).getHost())) {
				context = HttpClientContext.create();
				cookies = new ArrayList<>();
				charset = null;
			}
		}catch (Exception ex){
			errMsg = ex.getMessage();
		}
	}


	/*
	* build HttpClient by different proxies
	* disable Follow redirects cause They can not handle Url properly witch having Chinese and in other charsets
	* Ref: https://my.oschina.net/SmilePlus/blog/682198
	* */
	private HttpClient buildHttpClient(){
		HttpClient httpClient;
		RequestConfig config = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(15000).setCookieSpec(CookieSpecs.STANDARD).build();
		if(proxy_type == Proxy.Type.SOCKS){
			Registry<ConnectionSocketFactory> reg = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("http", new PlainConnectionSocketFactory(){
						@Override
						public Socket createSocket(HttpContext context) throws IOException {
							InetSocketAddress socksAddr = (InetSocketAddress) context.getAttribute("socks.address");
							Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksAddr);
							return new Socket(proxy);
						}
						@Override
						public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException {
							// Convert address to unresolved
							InetSocketAddress unresolvedRemote = InetSocketAddress
									.createUnresolved(host.getHostName(), remoteAddress.getPort());
							return super.connectSocket(connectTimeout, socket, host, unresolvedRemote, localAddress, context);
						}
					})
					.register("https", new SSLConnectionSocketFactory(SSLContexts.createSystemDefault()){
						@Override
						public Socket createSocket(HttpContext context) throws IOException {
							InetSocketAddress socksAddr = (InetSocketAddress) context.getAttribute("socks.address");
							Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksAddr);
							return new Socket(proxy);
						}
					})
					.build();

			PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(reg);
			httpClient = HttpClients.custom()
					.setDefaultRequestConfig(config)
					.setConnectionManager(cm)
					.disableRedirectHandling()
					.build();
			InetSocketAddress socksAddr = new InetSocketAddress(proxy_host, proxy_port);
			context.setAttribute("socks.address", socksAddr);

		}else if(proxy_type == Proxy.Type.HTTP){
			HttpHost proxy = new HttpHost(proxy_host, proxy_port, null);
			httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config)
					.setProxy(proxy)
					.disableRedirectHandling()
					.build();
		}else{ //direct
			httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).disableRedirectHandling().build();
		}
		return httpClient;
	}

	/*
	 *
	 * filter out unwanted Mime Type and get page charset
	 */
	private void parseHeaders(HttpEntity entity){
		ContentType contentType = ContentType.get(entity);
		String mimeType = contentType.getMimeType();
		if(mimeType != null && allowedMimeTypes.size()>0){
			if(!allowedMimeTypes.contains(mimeType)){
				errMsg = "MimeType("+mimeType+") not in allowedMimeTypes.";
				return;
			}
		}
		Charset cs = contentType.getCharset();
		if (cs != null) {
			charset = cs.toString();
			logger.debug("Detect charset in header:"+charset);
		}
	}


	/*
	* convert encoding to utf-8
	* Ref: http://justdo2008.iteye.com/blog/463753
	* */
	private void parseBody(HttpEntity entity){
		try {
			InputStream is = entity.getContent();
			ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
			byte[] temp = new byte[1024];
			int size;
			while ((size = is.read(temp)) != -1) {
				os.write(temp, 0, size);
			}
			if (charset == null) {
				Document doc = Jsoup.parse(os.toString());
				Elements metaTags = doc.getElementsByTag("meta");
				for (Element metaTag : metaTags) {
					String content = metaTag.attr("content");
					String http_equiv = metaTag.attr("http-equiv");
					charset = metaTag.attr("charset");
					if (!charset.isEmpty()) {
						logger.debug("Detect charset in meta:"+charset);
						break;
					}
					if (http_equiv.toLowerCase().equals("content-type")) {
						charset = content.substring(content.toLowerCase().indexOf("charset") + "charset=".length());
						logger.debug("Detect charset in http-equiv:"+charset);
						break;
					}
				}
				if (charset == null || charset.isEmpty())
					charset = "utf-8";
			}
			html = new String(os.toByteArray(), charset);
		}catch (Exception ex){
			errMsg = ex.getMessage();
		}
	}



	/*
	*
	* handle with white space and Chinese characters
	* From: http://www.jianshu.com/p/9be694c8fee2
	* */
	private String encodeUrl(String url){
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < url.length(); i++) {
			char c = url.charAt(i);
			if (c <= 255) {
				sb.append(c);
			} else {
				byte[] b;
				try {
					b = String.valueOf(c).getBytes("utf-8");
				} catch (Exception ex) {
					logger.warn(ex.getMessage());
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
		url = sb.toString();
		//fix url encode bug
		url = url.replaceAll(" ", "%20");
		return url;
	}
}
