package org.csip.bigData.crawler.distributed.util;

import org.junit.Test;
import us.codecraft.webmagic.Request;

/**
 * Created by bun@csip.org.cn on 2016/10/6.
 */
public class RedisUtilCSIPTest {
    String url="http://www.bjeit.gov.cn/jxdt/mtbd/113632.htm";
    String url2="http://www.bjeit.gov.cn/jxdt/mtbd/113673.htm";
    String url3="http://cria.mei.net.cn/news.asp?vid=3582";
    Request request=new Request(url2);
    String key1="www.bjeit.gov.cn";
    String key2="cria.mei.net.cn";
    //        String key3="cria.mei.net.cn";
    @Test
    public void pushToFetchList() throws Exception {

//        String key= StringUtil.reverseDomain(key2);
        String url2="http://www.robot-china.com/news/201609/30/36254.html";
        Request request=new Request(url2);
        RedisUtilCSIP.instance.pushToFetchList(request);

    }

//    @Test
//    public void pushToFetchList2() throws Exception {
//
//        String key= StringUtil.reverseDomain(key2);
//        RedisUtilCSIP.instance.pushToFetchList(request,key);
//
//    }

    @Test
    public void pollFromFetchList() throws Exception {
        String key= StringUtil.reverseDomain(key2);
        Request request=RedisUtilCSIP.instance.pollFromFetchList(key);
        System.out.println(request==null);
    }



    @Test
    public void pushToHistoryList() throws Exception {
        String url2="http://www.robot-china.com/news/201609/30/36254.html";
        Request request=new Request(url2);
        System.out.println(RedisUtilCSIP.instance.pushToPageHistoryHash(request,URLCrawlState.Being_Crawl));

    }

//    @Test
//    public void pushToHistoryList2() throws Exception {
//        String pageKey=StringUtil.reverseDomain(key1);
//        System.out.println(RedisUtilCSIP.instance.pushToPageHistoryHash(request,URLCrawlState.Being_Crawl,pageKey));
//
//    }



    //pagehistory不需要取，即使更新也不需要poll。
    @Test
    public void pollFromHistoryList() throws Exception {

    }

    @Test
    public void pushToExceptionList() throws Exception {

        String url2="http://www.robot-china.com/news/201609/30/36255.html";
        Request request=new Request(url2);
        RedisUtilCSIP.instance.pushToExceptionSortedSet(request);

    }
//    @Test
//    public void pushToExceptionList2() throws Exception {
//
//        String key= StringUtil.reverseDomain(key1);
//        RedisUtilCSIP.instance.pushToExceptionSortedSet(request,key);
//
//    }




//    @Test
//    public void pollFromExceptionList() throws Exception {
//        String urlStr="http://cria.mei.net.cn/news.asp?vid=3581";
//        URL url=new URL(urlStr);
//        Request request=new Request(urlStr);
//        String keyDomain=url.getHost();
//        System.out.println("domain:"+keyDomain);
//        String key=StringUtil.reverseDomain(keyDomain);
//        System.out.println("key:"+key);
//        RedisUtilCSIP.instance.pollFromExceptionSortedSet(request,key);
//
//
//    }

    @Test
    public void spiderCheckExceptionSortedSetTest() throws Exception {
        String url2="http://www.robot-china.com/news/201609/30/36259.html";
        String ReverseDomain=URLUtil.getReverseDomain(url2);
//        Request request=new Request(url2);
        int retryTimes=3;
        RedisUtilCSIP.instance.spiderCheckExceptionSortedSet(ReverseDomain,retryTimes);

    }


    @Test
    public void pollFromExceptionSortedSetTest() throws Exception {
        String url2="http://www.robot-china.com/news/201609/30/36254.html";
        String ReverseDomain=URLUtil.getReverseDomain(url2);
        Request request=new Request(url2);
        RedisUtilCSIP.instance.pollFromExceptionSortedSet(request);



    }

}