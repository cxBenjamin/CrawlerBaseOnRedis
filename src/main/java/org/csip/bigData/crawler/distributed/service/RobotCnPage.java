package org.csip.bigData.crawler.distributed.service;

import org.csip.bigData.crawler.distributed.RedisDownloaderCSIP;
import org.csip.bigData.crawler.distributed.RedisSpiderCSIP;
import org.csip.bigData.crawler.distributed.dao.MongoDBPipeline;
import org.csip.bigData.crawler.distributed.util.MD5Util;
import org.csip.bigData.crawler.distributed.util.ParamsConfigurationUtil;
import org.csip.bigData.crawler.distributed.util.dataProcessUtil.*;
import org.csip.bigData.crawler.distributed.util.spiderUtil.UserAgentUtil;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Selectable;

import java.io.IOException;

import java.util.*;

import static org.csip.bigData.crawler.distributed.util.spiderUtil.DateUtil.getSystemCurrentDateTime;

/**
 * Created by csip on 7/26/16.
 * 中国机器人网 www.robotcn.com.cn
 */
public class RobotCnPage implements PageProcessor {

    private static final List<String> URLlist = new ArrayList<String>();
    //行业新闻列表
    private static final String URL_NEWS_LIST = "http://www\\.robotcn\\.com\\.cn/news/list.php\\?catid=400&\\w+=\\d+";
    //企业报道列表
    private static final String URL_REPORT_LIST = "http://www\\.robotcn\\.com\\.cn/news/list.php\\?catid=403&\\w+=\\d+";
    //行业市场研究列表
    private static final String URL_RESEARCH_LIST = "http://www\\.robotcn\\.com\\.cn/news/list.php\\?catid=401&\\w+=\\d+";
    //新产品新技术列表
    private static final String URL_PRODUCT_LIST = "http://www\\.robotcn\\.com\\.cn/news/list.php\\?catid=402&\\w+=\\d+";
    //招标咨询列表
    private static final String URL_CONSULT_LIST = "http://www\\.robotcn\\.com\\.cn/news/list.php\\?catid=421&\\w+=\\d+";
    //热点招标列表
    private static final String URL_HOTSPOT_LIST = "http://www\\.robotcn\\.com\\.cn/news/list.php\\?catid=422&\\w+=\\d+";
    //资讯文章页
    private static final String URL_INFORMATION = "http://www\\.robotcn\\.com\\.cn/news/show.php\\?itemid=\\d+";

    //机器人行情列表
    private static final String URL_MARKET_LIST = "http://www\\.robotcn\\.com\\.cn/quote/list\\.php\\?catid=416&page=\\d+";
    //法律法规列表
    private static final String URL_LAW_LIST = "http://www\\.robotcn\\.com\\.cn/quote/list\\.php\\?catid=417&page=\\d+";
    //焦点行情列表
    private static final String URL_FOCUS_LIST = "http://www\\.robotcn\\.com\\.cn/quote/list\\.php\\?catid=418&page=\\d+";
    //其它行情列表
    private static final String URL_OTHER_LIST = "http://www\\.robotcn\\.com\\.cn/quote/list\\.php\\?catid=419&page=\\d+";
    //行情文章页
    private static final String URL_MARKET = "http://www\\.robotcn\\.com\\.cn/quote\\/show.php\\?itemid=\\d+";

    private Site site = Site.me().setRetryTimes(3).setSleepTime(8000).setUserAgent(UserAgentUtil.getUserAgent());

    private static Map<String, Object> getcontentTime(Selectable select, String regex) {
        Map<String, Object> cpMap = new HashMap<String, Object>();
        //获取列表页链接
        String listUrl = select.links().regex(regex).toString();
        //System.out.println("url:"+listUrl);
        //获取标题中的时间
        String contentTime = select.xpath("//span[@class=\"f_r px11 f_gray\"]/text()").toString();
        String contentUrl = select.xpath("//a/@href").toString();

        String date = contentTime + ":00";

        Request request = new Request(listUrl);
        request.putExtra("publish_time", date);
        cpMap.put("request", request);
        cpMap.put("url", contentUrl);
        cpMap.put("date", contentTime);
        return cpMap;
    }

    public void getandAddLinks(Page page, String urlRegex) {
        List<Selectable> listUrlList = page.getHtml().xpath("//div[@class='catlist']/ul/li[@class='catlist_li']").nodes();
        List<String> LinkList = new ArrayList<String>();
        //遍历列表page的节点
        for (Selectable select : listUrlList) {
            Map<String, Object> cMap = getcontentTime(select, urlRegex);
            //如果是更新的，就把这个内容页的链接加入到队列中，
            LinkList.add(cMap.get("url").toString());
            System.out.println("符合的链接：" + cMap.get("url").toString());
            page.addTargetRequest((Request) cMap.get("request"));
        }

        if (LinkList.size() == listUrlList.size()) {
            //获取下一页
            List<Selectable> pageNode = page.getHtml().xpath("//div[@class='pages']/a").nodes();
            for (Selectable node : pageNode) {
                if (node.xpath("//a/text()").toString().replaceAll("\\u00A0", "").equals("下一页»")) {
                    String link = node.xpath("//a/@href").toString();
                    System.out.println("获得下一页链接" + link);
                    page.addTargetRequest(link);
                    break;
                }
            }
        }


    }

    @Override
    public void process(Page page) {
        //行业新闻列表页
        if (page.getUrl().regex(URL_NEWS_LIST).match()) {
            getandAddLinks(page, URL_INFORMATION);
            //企业报道列表列表页
        } else if (page.getUrl().regex(URL_REPORT_LIST).match()) {
            getandAddLinks(page, URL_INFORMATION);
            //行业市场研究列表页
        } else if (page.getUrl().regex(URL_RESEARCH_LIST).match()) {
            getandAddLinks(page, URL_INFORMATION);
            //新产品新技术列表页
        } else if (page.getUrl().regex(URL_PRODUCT_LIST).match()) {
            getandAddLinks(page, URL_INFORMATION);
            //招标咨询列表页
        } else if (page.getUrl().regex(URL_CONSULT_LIST).match()) {
            getandAddLinks(page, URL_INFORMATION);
            //热点招标列表页
        } else if (page.getUrl().regex(URL_HOTSPOT_LIST).match()) {
            getandAddLinks(page, URL_INFORMATION);
            //机器人行情列表页
        } else if (page.getUrl().regex(URL_MARKET_LIST).match()) {
            getandAddLinks(page, URL_MARKET);
            //法律法规列表页
        } else if (page.getUrl().regex(URL_LAW_LIST).match()) {
            getandAddLinks(page, URL_MARKET);
            //焦点行情列表页
        } else if (page.getUrl().regex(URL_FOCUS_LIST).match()) {
            getandAddLinks(page, URL_MARKET);
            //其它行情列表页
        } else if (page.getUrl().regex(URL_OTHER_LIST).match()) {
            getandAddLinks(page, URL_MARKET);
            //咨询文章页
        } else if (page.getUrl().regex(URL_INFORMATION).match() || page.getUrl().regex(URL_MARKET).match()) {
            page.putField("id", "cn.com.robotcn.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
            page.putField("source_url", page.getUrl().toString());
            System.out.println("getURL:" + page.getUrl());
            String title = page.getHtml().xpath("//*[@id='title']/text()").toString();
            page.putField("title", title);
            System.out.println(title);
            //String publish = page.getHtml().xpath("/html/body/div[6]/div[1]/div/div[2]/text()").replace("发布日期：", "").replace("浏览次数：", "").toString();
            // page.putField("publish_time", trim(publish) + " 00:00:00");
            page.putField("publish_time", page.getRequest().getExtra("publish_time").toString());
            //System.out.println("publish_time:"+page.getRequest().getExtra("publish_time").toString());
            page.putField("source", "中国机器人网");
            page.putField("crawler_time", getSystemCurrentDateTime());
            page.putField("author", "中国机器人网");
            // page.putField("digest", "");
//            System.out.println("文章的来源字段："+page.getHtml().xpath("//ul[@id='plshare']/li[3]/text()").replace("来源： ","").toString());
            String content = page.getHtml().xpath("//div[@class='content']").toString();
            //去掉所有html标签的干净文本
            String cleanContent1 = CleanTextUtil.getCleanText(content);
            String cleanContent = cleanContent1.replaceAll("\\'\\&gt;", "");
            page.putField("cleanContent", cleanContent);
            try {
                SimHash simHash = new SimHash(cleanContent, 64);
                page.putField("simhash", simHash.get64strSimHash());
            } catch (IOException e) {
                e.printStackTrace();
            }
            ArrayList<String> segments = HanlpUtil.instance.getSegment(cleanContent);
            StringBuilder sb = new StringBuilder();
            for (String segment : segments) {
                sb.append(segment).append(" ");
            }
            page.putField("chineseSegment", sb.toString());
            String source_images = page.getHtml().xpath("//div[@id='article']/p/img/@original").all().toString();
            String[] imageArr = source_images.replace("[", "").replace("]", "").split(",");
            int imalength = imageArr.length;
            if (imageArr[imageArr.length - 1] == null || imageArr[imageArr.length - 1].equals("")) {
                imalength = 0;
            }
            for (int i = 0; i < imalength; i++) {
                String img_one = imageArr[i].trim();
                content = content.replace(img_one, "images[" + i + "]");
            }
            page.putField("content", ContentUtil.removeTags(content));
            page.putField("source_tag", "行业新闻");
            page.putField("images", page.getHtml().xpath("//div[@id='article']/p/img/@original").all().toString());
            page.putField("source_content", page.getHtml().toString());
            page.putField("from_site", "中国机器人网www.robotcn.com.cn");

            page.putField("category", ArticleCategoryUtil.instance.getCategory(title));
            page.putField("digest", HanlpUtil.instance.getPartWordsSummary(cleanContent));
            ArrayList<String> sourceTagList = new ArrayList<String>();
            int number = 10;
            ArrayList<String> keywords = KeywordExtractUtil.getKeyword(cleanContent, sourceTagList, number);
            HashSet<String> sets = new HashSet<String>();
            for (String keyword : keywords) {
                sets.add(keyword);
            }
            StringBuilder setSb = new StringBuilder();
            for (String set : sets) {
                setSb.append(set).append(" ");
            }
            page.putField("tag", setSb.toString().trim());
        }
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        //读取配置文件的示例

        String collection = ParamsConfigurationUtil.instance.getParamString("mongodb.collection.CopyRobotCn");

        //行业新闻
        URLlist.add("http://www.robotcn.com.cn/news/list.php?catid=400&page=1");
        //企业报道
        URLlist.add("http://www.robotcn.com.cn/news/list.php?catid=403&page=1");
        //行业市场研究
        URLlist.add("http://www.robotcn.com.cn/news/list.php?catid=401&page=1");
        //新产品新技术
        URLlist.add("http://www.robotcn.com.cn/news/list.php?catid=402&page=1");
        //招标咨询
        URLlist.add("http://www.robotcn.com.cn/news/list.php?catid=421&page=1");
        //热点招标
        URLlist.add("http://www.robotcn.com.cn/news/list.php?catid=422&page=1");
        //机器人行情
        URLlist.add("http://www.robotcn.com.cn/quote/list.php?catid=416&page=1");
        //法律法规
        URLlist.add("http://www.robotcn.com.cn/quote/list.php?catid=417&page=1");
        //焦点行情
        URLlist.add("http://www.robotcn.com.cn/quote/list.php?catid=418&page=1");
        //其它行情
        URLlist.add("http://www.robotcn.com.cn/quote/list.php?catid=419&page=1");

        for (String url : URLlist) {
            RedisSpiderCSIP.create(new RobotCnPage()).addUrl(url)
                    .setDownloader(new RedisDownloaderCSIP())
                    .addPipeline(new MongoDBPipeline(collection))
                    .run();
        }

    }

}
