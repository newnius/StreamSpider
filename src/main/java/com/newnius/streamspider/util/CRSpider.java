package com.newnius.streamspider.util;

import org.apache.http.*;
import org.apache.http.client.HttpClient;
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
 * @version 0.1.0(General) Dependencies: com.newnius.util.CRLogger
 *          com.newnius.util.CRMsg com.newnius.util.CRErrorCode
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
	 * content-type(optional)
	 */
	private String contentType = CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED;

	private String userAgent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201";

	/*
	 * cookie(optional) will automatically set
	 */
	private List<String> cookies;

	/*
	 * site url
	 *
	 */
	private String urlStr;

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


    private List<String> allowedMimeTypes;

	private Proxy.Type proxy_type = Proxy.Type.DIRECT;
	private String proxy_host;
	private int proxy_port;

	private HttpClientContext context;

	private String html;
    private int statusCode;
    private String errMsg;


	public String getHtml(){
		return html;
	}

    public int getStatusCode(){
        return statusCode;
    }

    public void setAllowedMimeTypes(List<String> allowedMimeTypes){
        this.allowedMimeTypes = allowedMimeTypes;
    }

	public CRSpider() {
		logger = CRLogger.getLogger(TAG);
		userAgent = UA.random();
		cookies = new ArrayList<>();
		context = HttpClientContext.create();
        allowedMimeTypes = new ArrayList<>();
	}

	public CRSpider setEncoding(String encoding) {
		this.encoding = encoding;
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

	public void setProxy(Proxy.Type type, String host, int port) {
		proxy_type = type;
		proxy_host = host;
		proxy_port = port;
	}

	public void doGet(String url) {
        /* clean */
        errMsg = null;
        statusCode = 0;

		try {
			HttpGet httpGet = new HttpGet();
			httpGet.setURI(new URI(url));
			httpGet.setHeader("User-Agent", userAgent);

			if(urlStr!=null && !new URL(urlStr).getHost().equals(new URL(url).getHost())){
				context = HttpClientContext.create();
				cookies = new ArrayList<>();
                logger.debug("Clean Cookie");
			}

			for(String cookie: cookies){
				httpGet.addHeader("Cookie", cookie);
			}

			HttpClient httpClient = getHttpClient();

			HttpResponse response = httpClient.execute(httpGet, context);

			statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200){
                errMsg = "Status Code is "+statusCode;
                return;
            }
			parse(response);
			List<URI> uris = context.getRedirectLocations();
			if(uris!=null && uris.size()>0) {
				urlStr = uris.get(uris.size()-1).toString();
			}
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

	public CRMsg doPost(final String postdata) {
		try {
			URL url = new URL(urlStr);
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

			BufferedReader br = new BufferedReader(new InputStreamReader(is, encoding));
			String response = "";
			String readLine;
			while ((readLine = br.readLine()) != null) {
				response = response + readLine;
			}
			headers = conn.getHeaderFields();

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

	private HttpClient getHttpClient(){
		HttpClient httpClient;
		if(proxy_type == Proxy.Type.SOCKS){
			Registry<ConnectionSocketFactory> reg = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("http", new PlainConnectionSocketFactory(){
						@Override
						public Socket createSocket(HttpContext context) throws IOException {
							InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute("socks.address");
							Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
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
							InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute("socks.address");
							Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
							return new Socket(proxy);
						}
					})
					.build();

			PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(reg);
			httpClient = HttpClients.custom()
					.setConnectionManager(cm)
					.build();
			InetSocketAddress socksaddr = new InetSocketAddress(proxy_host, proxy_port);
			context.setAttribute("socks.address", socksaddr);

		}else if(proxy_type == Proxy.Type.HTTP){
			HttpHost proxy = new HttpHost(proxy_host, proxy_port, null);
			RequestConfig config = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(15000).build();
			httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config)
					.setProxy(proxy)
					.build();
			//.disableRedirectHandling()
		}else{ //direct
			RequestConfig config = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(15000).build();
			httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
		}
		return httpClient;
	}

	private void parse(HttpResponse response){
		try {

			HttpEntity entity = response.getEntity();
			if(entity==null){
				return;
			}

			String charset = null;
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

    public String getErrMsg() {
        return errMsg;
    }
}
