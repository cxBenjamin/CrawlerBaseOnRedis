package org.csip.bigData.crawler.distributed.service;

import org.csip.bigData.crawler.distributed.RedisDownloaderCSIP;
import org.csip.bigData.crawler.distributed.RedisSpiderCSIP;
import org.csip.bigData.crawler.distributed.util.spiderUtil.UserAgentUtil;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author code4crafter@gmail.com <br>
 */
public class SinaBlogProcessor implements PageProcessor {

    public static final String URL_LIST1 = "http://blog\\.sina\\.com\\.cn/s/articlelist_1487828712_0_\\d+\\.html";
    public static final String URL_LIST3 = "http://blog\\.sina\\.com\\.cn/s/articlelist_1300871220_0_\\d+\\.html";
    public static final String URL_POST = "http://blog\\.sina\\.com\\.cn/s/blog_\\w+\\.html";

    //.setRetryTimes(3)的参数和ExceptionrettyTimes共用了，主要是减少太多的代码改动。重试机制在ExceptionLIst中实现。
    private Site site = Site
            .me()
            .setDomain("blog.sina.com.cn")
            .setSleepTime(3000).setRetryTimes(2)
            .setUserAgent(UserAgentUtil.getUserAgent());

    @Override
    public void process(Page page) {
        //列表页
        if (page.getUrl().regex(URL_LIST1).match()) {
            page.addTargetRequests(page.getHtml().xpath("//div[@class=\"articleList\"]").links().regex(URL_POST).all());
            page.addTargetRequests(page.getHtml().links().regex(URL_LIST1).all());
            //文章页
        } else {
            page.putField("title", page.getHtml().xpath("//div[@class='articalTitle']/h2"));
//            page.putField("content", page.getHtml().xpath("//div[@id='articlebody']//div[@class='articalContent']"));
//            page.putField("date",
//                    page.getHtml().xpath("//div[@id='articlebody']//span[@class='time SG_txtc']").regex("\\((.*)\\)"));
        }
    }

    @Override
    public Site getSite() {
        return site;
    }

    //以这个简单的例子为例。
    public static void main(String[] args) {
        String url1="http://blog.sina.com.cn/s/articlelist_1487828712_0_1.html";
        String url2="http://blog.sina.com.cn/s/articlelist_1487828712_0_2.html";
        String url3="http://blog.sina.com.cn/s/articlelist_1300871220_0_1.html";
        List<String> urls=new ArrayList<String>();
        for (int i = 0; i < 131; i++) {
            String url="http://blog.sina.com.cn/s/articlelist_1300871220_0_"+String.valueOf(i+1)+".html";
            urls.add(url);
        }
        System.out.println("urls len:"+urls.size());
//        RedisSpiderCSIP.create(new SinaBlogProcessor()).startUrls(urls).setDownloader(new RedisDownloaderCSIP())
//                .run();
        RedisSpiderCSIP.create(new SinaBlogProcessor()).addUrl(url1).setDownloader(new RedisDownloaderCSIP())
                .run();
    }
}
