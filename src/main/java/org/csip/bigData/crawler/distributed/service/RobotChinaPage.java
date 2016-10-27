package org.csip.bigData.crawler.distributed.service;


import org.csip.bigData.crawler.distributed.RedisDownloaderCSIP;
import org.csip.bigData.crawler.distributed.RedisSpiderCSIP;
import org.csip.bigData.crawler.distributed.dao.MongoDBPipeline;
import org.csip.bigData.crawler.distributed.util.MD5Util;
import org.csip.bigData.crawler.distributed.util.ParamsConfigurationUtil;
import org.csip.bigData.crawler.distributed.util.dataProcessUtil.*;
import org.csip.bigData.crawler.distributed.util.spiderUtil.DateUtil;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Selectable;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 * Created by csip on 7/26/16.
 * 中国机器人网（www.robot-china.com）
 */
public class RobotChinaPage implements PageProcessor {

    //资讯列表（行业、展会）
    private static final String URL_NEWS_LIST = "http://www\\.robot-china\\.com/news/list-\\d+-\\d+.html";
    //专题列表(没有发布时间，增量方案需修改)
    private static final String URL_TOPIC_LIST = "http://www\\.robot-china\\.com/zhuanti/list-\\d+-\\d+.html";
    //企业新闻列表
    private static final String URL_COMPANY_NEWS_LIST = "http://www\\.robot-china\\.com/company/news-htm-page-\\d+.html";
    //资讯文章页
    private static final String URL_NEWS_ARTICLE = "http://www\\.robot-china\\.com/news/\\d+/\\d+/\\d+.html";
    //专题文章页
    private static final String URL_TOPIC_ARTICLE = "http://www\\.robot-china\\.com/zhuanti/show-\\d+.html";
    //企业新闻文章页
    private static final String URL_COMPANY_ARTICLE = "http://\\w+\\.robot-china\\.com/news/itemid-\\d+.shtml";


    private Site site = Site.me().setRetryTimes(3).setSleepTime(300);

    @Override
    public void process(Page page) {
        //资讯列表页
        if (page.getUrl().regex(URL_NEWS_LIST).match()) {
            /*page.addTargetRequests(page.getHtml().xpath("//div[@class=\"panel-container\"]").links().regex(URL_NEWS_ARTICLE).all());
            List<String> links_post = page.getHtml().xpath("//div[@class=\"pages\"]").links().regex(URL_NEWS_LIST).all();
            page.addTargetRequests(links_post);*/
            increaseUpdate(page, URL_NEWS_ARTICLE);
            //专题列表页
        } else if (page.getUrl().regex(URL_TOPIC_LIST).match()) {
            page.addTargetRequests(page.getHtml().xpath("//div[@class='panel-container']").links().regex(URL_TOPIC_ARTICLE).all());
            List<String> links_post = page.getHtml().xpath("//div[@class='pages']").links().regex(URL_TOPIC_LIST).all();
            page.addTargetRequests(links_post);
////            increaseUpdate(page, URL_TOPIC_ARTICLE);
            //企业新闻列表页
        } else if (page.getUrl().regex(URL_COMPANY_NEWS_LIST).match()) {
            /*page.addTargetRequests(page.getHtml().xpath("//div[@class=\"catlist\"]").links().regex(URL_COMPANY_ARTICLE).all());
            List<String> links_post = page.getHtml().xpath("//div[@class=\"pages\"]").links().regex(URL_COMPANY_NEWS_LIST).all();
            page.addTargetRequests(links_post);*/
            increaseUpdate(page, URL_COMPANY_ARTICLE);
            //资讯和专题文章页
        } else {
            if (page.getUrl().regex(URL_NEWS_ARTICLE).match() || page.getUrl().regex(URL_TOPIC_ARTICLE).match()) {
                page.putField("id", "com.robot-china.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
                page.putField("source_url", page.getUrl().toString());
                String title = page.getHtml().xpath("//div[@class='zx3']/h3/text()").toString();
                page.putField("title", title);
                String publishTime = page.getHtml().xpath("//ul[@id='plshare']/li[2]/text()").toString();
                page.putField("publish_time", publishTime.replace("时间：", "") + " 00:00:00");

                String sourceInfo = page.getHtml().xpath("//ul[@id='plshare']/li[3]/text()").toString();
                if (sourceInfo.contains("来源")) {
                    page.putField("source", sourceInfo.replace("来源：", "").trim());
                    page.putField("author", page.getHtml().xpath("//ul[@id='plshare']/li[4]/text()").replace("编译：", "").toString());
                } else {
                    //System.out.println("来源网站");
                    page.putField("source", "中国机器人网");
                    page.putField("author", sourceInfo.replace("编译：", ""));
                }
                page.putField("crawler_time", DateUtil.getSystemCurrentDateTime());
                //page.putField("digest", "");

                String content = page.getHtml().xpath("//div[@class='content']").toString();
                //去掉所有html标签的干净文本
                String cleanContent = CleanTextUtil.getCleanText(content);
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

                page.putField("source_tag", page.getHtml().xpath("//ul[@id='plshare']/li[1]/a/text()").toString());
                //page.putField("category", "动态");
                String imageStr = page.getHtml().xpath("//div[@class='content']//img/@src").all().toString();
                String[] imageArr = imageStr.replace("[", "").replace("]", "").split(",");
                int imalength = imageArr.length;
                if (imageArr[imageArr.length - 1] == null || imageArr[imageArr.length - 1].equals("")) {
                    imalength = 0;
                }
                for (int i = 0; i < imalength; i++) {
                    String img_one = imageArr[i].trim();
                    //int positon = Arrays.binarySearch(imageArr, img_one);
                    content = content.replace(img_one, "images[" + i + "]");
                }
                page.putField("content", ContentUtil.removeTags(content));
                page.putField("images", imageStr);

                page.putField("source_content", page.getRawText());
                page.putField("from_site", "中国机器人网www.robot-china.com");

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
                //企业新闻文章页
            } else if (page.getUrl().regex(URL_COMPANY_ARTICLE).match()) {
                page.putField("id", "com.robot-china.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
                page.putField("source_url", page.getUrl().toString());
                String title = page.getHtml().xpath("//div[@class='title']/text()").toString();
                page.putField("title", title);
                String publishTime = page.getHtml().xpath("//div[@class='info']/text()").toString();
                page.putField("publish_time", publishTime.replace("发布时间：", "").substring(0, 10) + " 00:00:00");
//                System.out.println("内容页："+DateUtil.getSystemCurrentDateTime());
                page.putField("source", "中国机器人网");
                page.putField("crawler_time", DateUtil.getSystemCurrentDateTime());
                page.putField("author", "中国机器人网");
                //page.putField("digest", "");

                String content = page.getHtml().xpath("//div[@class='content']").toString();
                //去掉所有html标签的干净文本
                String cleanContent = CleanTextUtil.getCleanText(content);
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
                String imageStr = page.getHtml().xpath("//div[@class='content']//img/@src").all().toString();
                String[] imageArr = imageStr.replace("[", "").replace("]", "").split(",");
                int imalength = imageArr.length;
                if (imageArr[imageArr.length - 1] == null || imageArr[imageArr.length - 1].equals("")) {
                    imalength = 0;
                }
                for (int i = 0; i < imalength; i++) {
                    String img_one = imageArr[i].trim();
                    //int positon = Arrays.binarySearch(imageArr, img_one);
                    content = content.replace(img_one, "images[" + i + "]");
                }

                page.putField("content", ContentUtil.removeTags(content));

                page.putField("source_tag", "企业新闻");
                //page.putField("category", "动态");
                page.putField("images", page.getHtml().xpath("//div[@class='content']//img/@src").all().toString());
                page.putField("source_content", page.getRawText());
                page.putField("from_site", "中国机器人网www.robot-china.com");
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
    }

    @Override
    public Site getSite() {
        return site;
    }


    private static void increaseUpdate(Page page, String articleRegex) {

        String nextPage = "下一页";

        int nodeNum = 0;
        //资讯文章页
        if (URL_NEWS_ARTICLE.equals(articleRegex)) {
            //获取内容页的链接所在的节点
            List<Selectable> selectableList = page.getHtml().xpath("//div[@id='tab']/ul").nodes();
            ArrayList<String> urlList = new ArrayList<String>();
            nodeNum = selectableList.size();
            for (Selectable selectable : selectableList) {
                //获取内容页链接
                String url = selectable.links().regex(articleRegex).toString();

                urlList.add(url);
                page.addTargetRequest(url);

            }

            if (urlList.size() == nodeNum) {
                List<Selectable> listUrlList = page.getHtml().xpath("//*[@class='pages']/a").nodes();
                //遍历列表page的节点
                for (Selectable select : listUrlList) {
                    //获取列表页链接
                    String listUrl = select.xpath("//a/@href").toString();

                    String listText = select.xpath("//a/text()").toString();

                    //如果是“下一页”，就把这个链接加入到队列中。
                    if (listText.equals(nextPage)) {
                        page.addTargetRequest(listUrl);
                        System.out.println("listUrl:" + listUrl);
                        System.out.println("listText:" + listText);
                    }
                }
            }
            //企业新闻文章页
        } else if (URL_COMPANY_ARTICLE.equals(articleRegex)) {
            //获取内容页的链接所在的节点
            List<Selectable> selectableList = page.getHtml().xpath("//div[@class='catlist']/ul/li").nodes();
            ArrayList<String> urlList = new ArrayList<String>();
            nodeNum = selectableList.size();
            for (Selectable selectable : selectableList) {
                //获取内容页链接
                String url = selectable.links().regex(articleRegex).toString();
                urlList.add(url);
                page.addTargetRequest(url);

            }

            if (urlList.size() == nodeNum) {
                List<Selectable> listUrlList = page.getHtml().xpath("//*[@class='pages']/a").nodes();
                //遍历列表page的节点
                for (Selectable select : listUrlList) {
                    //获取列表页链接
                    String listUrl = select.xpath("//a/@href").toString();
                    String listText = select.xpath("//a/text()").toString();
                    //如果是“下一页”，就把这个链接加入到队列中。
                    if (listText.equals(nextPage)) {
                        page.addTargetRequest(listUrl);
                        System.out.println("listUrl:" + listUrl);
                        System.out.println("listText:" + listText);
                    }
                }
            }
        }

    }

    public static void main(String[] args) {

        //读取配置文件的示例
        String collection = ParamsConfigurationUtil.instance.getParamString("mongodb.collection.CopyRobotChina");

        //分类id依次对应：行业资讯，展会资讯
        String[] newsCatArr = new String[]{"937", "1140"};
        //分类id依次对应：法律专题，行业专题，技术专题，企业专题，展会专题
        String[] topicCatArr = new String[]{"1429", "187", "188", "189", "190"};
//        for (String aNewsCatArr : newsCatArr) {
//
//            RedisSpiderCSIP.create(new RobotChinaPage()).addUrl("http://www.robot-china.com/news/list-" + aNewsCatArr + "-1.html")
//                    .setDownloader(new RedisDownloaderCSIP())
//                    .addPipeline(new MongoDBPipeline(collection))
//                    .run();
//        }
//        for (String aTopicCatArr : topicCatArr) {
//
//            RedisSpiderCSIP.create(new RobotChinaPage()).addUrl("http://www.robot-china.com/zhuanti/list-" + aTopicCatArr + "-1.html")
//                    .setDownloader(new RedisDownloaderCSIP())
//                    .addPipeline(new MongoDBPipeline(collection))
//                    .run();
//        }
        //企业新闻

        RedisSpiderCSIP.create(new RobotChinaPage()).addUrl("http://www.robot-china.com/company/news-htm-page-1.html")
                .setDownloader(new RedisDownloaderCSIP())
                .addPipeline(new MongoDBPipeline(collection))
                .run();


    }

}

