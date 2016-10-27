package org.csip.bigData.crawler.distributed.util.spiderUtil;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.JsonFilePipeline;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import
//这个方法不太好，目前还找不到有效的代理IP。
//后面可能考虑购买一些代理IP。
public class IPProxyUtil implements PageProcessor {

    private static final String ipProxy_url= "http://www.xicidaili.com/nn/\\d+";

    private Site site = Site.me().setRetryTimes(3).setSleepTime(1000).setUserAgent(UserAgentUtil.getUserAgent());


    public static HttpClient getHttpClient(String url,int port)
    {
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(2000)
                .setConnectTimeout(2000)
                .setConnectionRequestTimeout(2000)
                .setStaleConnectionCheckEnabled(true)
                .build();
        CloseableHttpClient httpClient;
        HttpHost proxy=new HttpHost(url,port);
        DefaultRoutePlanner routePlanner=new DefaultProxyRoutePlanner(proxy);
        httpClient=HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).setRoutePlanner(routePlanner).build();
//        httpClient.getParams().setParameter(ConnRouteParams.DEFAULT_PROXY,proxy);
        return httpClient;

    }

    public boolean verifyURL(String url,int port)
    {

        boolean flag=false;
        HttpGet httpGet=new HttpGet("http://ip.chinaz.com/getip.aspx");
        HttpClient httpClient=getHttpClient(url,port);
//        httpClient.
        try {
            HttpResponse httpResponse=httpClient.execute(httpGet);
            if(httpResponse.getStatusLine().getStatusCode()==200)
            {
                flag=true;
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }


    @Override
    public void process(Page page) {
        //*[@id="ip_list"]
//        //*[@id="ip_list"]/tbody/tr[2]/td[2]
        Document doc= Jsoup.parse(page.getHtml().toString());
//        System.out.println(doc.toString());
        Elements trs =doc.select("table").select("tr");
        for(int i=0;i<trs.size();i++)
        {
            Elements tds = trs.get(i).select("td");
            if(tds.size()>2) {
                String url = tds.get(1).text();
                int port = Integer.parseInt(tds.get(2).text());
                System.out.println("url:" + url);
                System.out.println("port:" + port);
                boolean validURL=verifyURL(url,port);
                if(validURL)
                {

                    page.putField("url",url);
                    page.putField("port",port);
                }
            }

        }


    }

    @Override
    public Site getSite()
    {
        return site;
    }
    public static void main(String[] args) {

        List<String> links=new ArrayList<String>();
        String half_url="http://www.xicidaili.com/nn/";

        int size=10;
        for(int i=1;i<=size;i++)
        {
            String url=half_url+String.valueOf(i);
            links.add(url);
        }

        Spider.create(new IPProxyUtil()).startUrls(links).addPipeline(new JsonFilePipeline("/home/csip/Documents/bigData/project/data2/")).thread(5).run();

    }



}
