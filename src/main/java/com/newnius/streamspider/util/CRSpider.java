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
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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

	private long maxLength = 200000;

	private Proxy.Type proxy_type = Proxy.Type.DIRECT;

	private String proxy_host;

	private int proxy_port;

	private HttpClientContext context;

	private String html;

	private int statusCode;

	private String errMsg;

    private int redirectsCount = 0;

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

    public String getCharset() {
        return charset;
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
		clean(url);
		try {
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
                redirectsCount++;
                if(redirectsCount > 5){
                    errMsg = "Maximum Redirects Exceeded.";
                    return;
                }
				String newUrl = response.getFirstHeader("Location").getValue();
				url = new URL(new URL(url), newUrl).toString();
				url = new String(url.getBytes("ISO-8859-1"), "utf-8");
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
			errMsg = ex.getClass().getSimpleName()+":"+ex.getMessage();
		}
	}

	/*
	* clean cookie of another site
	* clean errMsg, status code
	* */
	private void clean(String url){
		if(statusCode!=301 && statusCode !=302) {
			redirectsCount = 0;
		}
		statusCode = 0;
		errMsg = null;
		try {
			if (currentStr != null && !new URL(currentStr).getHost().equals(new URL(url).getHost())) {
				context = HttpClientContext.create();
				cookies = new ArrayList<>();
				charset = null;
			}
		}catch (Exception ex){
			errMsg = ex.getClass().getSimpleName()+":"+ex.getMessage();
		}
	}


	/*
	* build HttpClient by different proxies
	* disable Follow redirects cause They can not handle Url properly witch having Chinese and in other charsets
	* Ref: https://my.oschina.net/SmilePlus/blog/682198
	* */
	private HttpClient buildHttpClient(){
		HttpClient httpClient;
		RequestConfig config = RequestConfig.custom().setConnectTimeout(10000).setSocketTimeout(15000).setCookieSpec(CookieSpecs.DEFAULT).setRedirectsEnabled(false).build();
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
					.build();
			InetSocketAddress socksAddr = new InetSocketAddress(proxy_host, proxy_port);
			context.setAttribute("socks.address", socksAddr);

		}else if(proxy_type == Proxy.Type.HTTP){
			HttpHost proxy = new HttpHost(proxy_host, proxy_port, null);
			httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config)
					.setProxy(proxy)
					.build();
		}else{ //direct
			httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
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
		long length = entity.getContentLength();
		if(maxLength!=0 && length>maxLength ){
			errMsg = "Length("+length+") greater than maxLength.";
			return;
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
			byte[] bytes = EntityUtils.toByteArray(entity);
			if (charset == null) {
				Document doc = Jsoup.parse(new String(bytes));
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
			html = new String(bytes, charset);
		}catch (Exception ex){
			errMsg = ex.getClass().getSimpleName()+":"+ex.getMessage();
		}
	}

}
