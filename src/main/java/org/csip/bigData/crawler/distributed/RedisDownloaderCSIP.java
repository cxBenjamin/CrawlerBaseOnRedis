package org.csip.bigData.crawler.distributed;

import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.csip.bigData.crawler.distributed.util.RedisUtilCSIP;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.AbstractDownloader;
import us.codecraft.webmagic.downloader.HttpClientGenerator;
import us.codecraft.webmagic.selector.PlainText;
import us.codecraft.webmagic.utils.HttpConstant;
import us.codecraft.webmagic.utils.UrlUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by bun@csip.org.cn on 2016/10/3.
 */
//下载这边的修改。主要是加入到pagehistotyList和exceptionList中。
public class RedisDownloaderCSIP extends AbstractDownloader {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, CloseableHttpClient> httpClients = new HashMap<String, CloseableHttpClient>();

    private HttpClientGenerator httpClientGenerator = new HttpClientGenerator();

    private CloseableHttpClient getHttpClient(Site site) {
        if (site == null) {
            return httpClientGenerator.getClient(null);
        }
        String domain = site.getDomain();
        CloseableHttpClient httpClient = httpClients.get(domain);
        if (httpClient == null) {
            synchronized (this) {
                httpClient = httpClients.get(domain);
                if (httpClient == null) {
                    httpClient = httpClientGenerator.getClient(site);
                    httpClients.put(domain, httpClient);
                }
            }
        }
        return httpClient;
    }



//这个类里面其他的方法都是为这个方法服务的。
    @Override
    public Page download(Request request, Task task) {
        //这里要加上exceptionList的处理。
        //页面正在处理的时候，将pagehistory中的状态更新为正在抓取、
        // 页面处理成功，将pagehistory状态更新为已抓取状态。
        Site site=null;
//        RedisUtilCSIP redisUtilCSIP=new RedisUtilCSIP();
        if(task!=null)
        {
            //添加的UUID我还没有想好怎么使用。
            site=task.getSite();
        }
        //目前还不明白为什么这里采用set类型。是不是只要返回值为200就行了。
        Set<Integer> acceptStatCode;
        String charset=null;
        Map<String,String> headers=null;
        if(site!=null)
        {
            //默认是200，表明返回的数据正确，
            //问题是为什么要在这里进行判断呢？
            //明明方法里什么都没有呀。
            acceptStatCode=site.getAcceptStatCode();
            charset=site.getCharset();
            headers=site.getHeaders();

        }
        else
        {
            //这句话是什么意思，为什么为空的时候也要设置为200.
            acceptStatCode= Sets.newHashSet(200);

        }
        logger.info("downloading page {}", request.getUrl());
        //这里写入pagehistoryList中，状态是“正在抓取”。

//        System.out.println("pagehistory正在抓取");



        CloseableHttpResponse httpResponse=null;
        int statusCode=0;
        //前面的都是初始化工作。不用在意。

        try {
            //抓起url的配置信息的设置。比如请求方式，超时，代理的设置。
            HttpUriRequest httpUriRequest = getHttpUriRequest(request, site, headers);
            //调用httpclient方法来获取URL数据。
            httpResponse =  getHttpClient(site).execute(httpUriRequest);
            //获取返回的状态码，200,404,501等等。
            statusCode=httpResponse.getStatusLine().getStatusCode();
            request.putExtra(Request.STATUS_CODE,statusCode);
            //意思是说，只接受200，其他状态都报错，
            //可以将出错信息写入到redis中exceptionList中
            System.out.println("code:"+statusCode);
            if(statusAccept(acceptStatCode, statusCode))
            {
                //获取的是整个html页面。如果上面的if语句下，貌似statecode都是200.那为什么还有设置page.stateCode呢。

                Page page = handleResponse(request, charset, httpResponse, task);


                onSuccess(request);
                return page;
            } else {


                logger.warn("code error " + statusCode + "\t" + request.getUrl());
                return null;
            }
        }catch(IOException e)
        {
            //这里的处理还没有想清楚，待解决。
            logger.warn("download page " + request.getUrl() + " error", e);
            //这里要处理为设定抓取次数，超过这个次数抓取仍然不成功，则采用pollFromException方法，设置pagehistoryLIst中为抓取失败。
            if (site.getCycleRetryTimes()>0)
            {
                //这里设置出错循环处理。也是要写到exceptionList中的。
                return addToCycleRetry(request,site);
            }



            //放弃抓取，从Exception中出队列，并且在pagehistoryList中状态记录为“放弃抓取”
            RedisUtilCSIP.instance.pollFromExceptionSortedSet(request);
            onError(request);
            return null;
        }
        finally {
            //不做任何处理。
            request.putExtra(Request.STATUS_CODE,statusCode);
            try {
                if (httpResponse !=null)
                {
                    //其功能就是关闭HttpEntity
                    EntityUtils.consume(httpResponse.getEntity());
                }
            }catch (IOException e)
            {
                logger.warn("close response fail", e);
            }
        }


    }

    @Override
    public void setThread(int thread) {
        httpClientGenerator.setPoolSize(thread);
    }
    //目前只有200是正常情况。
    protected boolean statusAccept(Set<Integer> acceptStatCode, int statusCode) {
        return acceptStatCode.contains(statusCode);
    }

    //这里就是设置一些抓取网页的配置信息的。
    protected HttpUriRequest getHttpUriRequest(Request request, Site site, Map<String, String> headers) {
        //这个是如何识别get，post等方法的？
        RequestBuilder requestBuilder = selectRequestMethod(request).setUri(request.getUrl());
        if (headers != null) {
            for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
                requestBuilder.addHeader(headerEntry.getKey(), headerEntry.getValue());
            }
        }
        //爬虫抓取的一些配置信息。
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setConnectionRequestTimeout(site.getTimeOut())
                .setSocketTimeout(site.getTimeOut())
                .setConnectTimeout(site.getTimeOut())
                .setCookieSpec(CookieSpecs.BEST_MATCH);
        //设置代理。
        if (site.getHttpProxyPool() != null && site.getHttpProxyPool().isEnable()) {
            HttpHost host = site.getHttpProxyFromPool();
            requestConfigBuilder.setProxy(host);
            request.putExtra(Request.PROXY, host);
        }else if(site.getHttpProxy()!= null){
            HttpHost host = site.getHttpProxy();
            requestConfigBuilder.setProxy(host);
            request.putExtra(Request.PROXY, host);
        }
        requestBuilder.setConfig(requestConfigBuilder.build());
        return requestBuilder.build();
    }


    protected RequestBuilder selectRequestMethod(Request request) {
        //不是特别明白，明明没有值的。
        //实际使用中不知道这个是怎么用的？
        String method = request.getMethod();
        if (method == null || method.equalsIgnoreCase(HttpConstant.Method.GET)) {
            //default get
            return RequestBuilder.get();
        } else if (method.equalsIgnoreCase(HttpConstant.Method.POST)) {
            RequestBuilder requestBuilder = RequestBuilder.post();
            NameValuePair[] nameValuePair = (NameValuePair[]) request.getExtra("nameValuePair");
            if (nameValuePair != null && nameValuePair.length > 0) {
                requestBuilder.addParameters(nameValuePair);
            }
            return requestBuilder;
        } else if (method.equalsIgnoreCase(HttpConstant.Method.HEAD)) {
            return RequestBuilder.head();
        } else if (method.equalsIgnoreCase(HttpConstant.Method.PUT)) {
            return RequestBuilder.put();
        } else if (method.equalsIgnoreCase(HttpConstant.Method.DELETE)) {
            return RequestBuilder.delete();
        } else if (method.equalsIgnoreCase(HttpConstant.Method.TRACE)) {
            return RequestBuilder.trace();
        }
        throw new IllegalArgumentException("Illegal HTTP Method " + method);
    }

    //处理请求回来的数据。主要是填空page的四个属性值。rawtext,url,request,statuscode.没有涉及到返回的数据的处理
    protected Page handleResponse(Request request, String charset, HttpResponse httpResponse, Task task) throws IOException {
        //为了获得完整的html页面，涉及到具体编码的处理，
        //以下的两个方法都是这个作用。
        String content = getContent(charset, httpResponse);
        Page page = new Page();
        page.setRawText(content);
        page.setUrl(new PlainText(request.getUrl()));
        page.setRequest(request);
        page.setStatusCode(httpResponse.getStatusLine().getStatusCode());
        return page;
    }

    protected String getContent(String charset, HttpResponse httpResponse) throws IOException {
        if (charset == null) {
            byte[] contentBytes = IOUtils.toByteArray(httpResponse.getEntity().getContent());
            String htmlCharset = getHtmlCharset(httpResponse, contentBytes);
            if (htmlCharset != null) {
                return new String(contentBytes, htmlCharset);
            } else {
                logger.warn("Charset autodetect failed, use {} as charset. Please specify charset in Site.setCharset()", Charset.defaultCharset());
                return new String(contentBytes);
            }
        } else {
            return IOUtils.toString(httpResponse.getEntity().getContent(), charset);
        }
    }

    protected String getHtmlCharset(HttpResponse httpResponse, byte[] contentBytes) throws IOException {
        String charset;
        // charset
        // 1、encoding in http header Content-Type
        String value = httpResponse.getEntity().getContentType().getValue();
        charset = UrlUtils.getCharset(value);
        if (StringUtils.isNotBlank(charset)) {
            logger.debug("Auto get charset: {}", charset);
            return charset;
        }
        // use default charset to decode first time
        Charset defaultCharset = Charset.defaultCharset();
        String content = new String(contentBytes, defaultCharset.name());
        // 2、charset in meta
        if (StringUtils.isNotEmpty(content)) {
            Document document = Jsoup.parse(content);
            Elements links = document.select("meta");
            for (Element link : links) {
                // 2.1、html4.01 <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
                String metaContent = link.attr("content");
                String metaCharset = link.attr("charset");
                if (metaContent.indexOf("charset") != -1) {
                    metaContent = metaContent.substring(metaContent.indexOf("charset"), metaContent.length());
                    charset = metaContent.split("=")[1];
                    break;
                }
                // 2.2、html5 <meta charset="UTF-8" />
                else if (StringUtils.isNotEmpty(metaCharset)) {
                    charset = metaCharset;
                    break;
                }
            }
        }
        logger.debug("Auto get charset: {}", charset);
        // 3、todo use tools as cpdetector for content decode
        return charset;
    }

}
