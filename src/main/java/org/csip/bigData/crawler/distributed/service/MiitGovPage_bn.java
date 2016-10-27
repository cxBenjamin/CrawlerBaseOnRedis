package org.csip.bigData.crawler.distributed.service;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.csip.bigData.crawler.distributed.RedisDownloaderCSIP;
import org.csip.bigData.crawler.distributed.RedisSpiderCSIP;
import org.csip.bigData.crawler.distributed.dao.MongoDBPipeline;
import org.csip.bigData.crawler.distributed.util.MD5Util;
import org.csip.bigData.crawler.distributed.util.ParamsConfigurationUtil;
import org.csip.bigData.crawler.distributed.util.StringUtil;
import org.csip.bigData.crawler.distributed.util.dataProcessUtil.*;
import org.csip.bigData.crawler.distributed.util.spiderUtil.DateUtil;
import org.csip.bigData.crawler.distributed.util.spiderUtil.PostUtil;
import org.csip.bigData.crawler.distributed.util.spiderUtil.UserAgentUtil;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 * Created by csip on 7/29/16.
 * 工信部（www.miit.gov.cn/）
 */
public class MiitGovPage_bn implements PageProcessor {
//    private static String customTime = null;

    private Site site = Site.me().setRetryTimes(3).setSleepTime(300).setUserAgent(UserAgentUtil.getUserAgent());
    private static final String URL_POST1 = "http://www\\.miit\\.gov\\.cn/\\w+/\\w+/\\w+/\\w+/\\w+/\\w+/content.html";
    //http://www.miit.gov.cn/n973401/n4961636/n4961670/c5192426/content.html
    private static final String URL_POST2 = "http://www\\.miit\\.gov\\.cn/\\w[0-9]{6}/\\w[0-9]{7}/\\w[0-9]{7}/\\w[0-9]{7}/content.html";
    private static final String URL_POST3 = "http://www\\.miit\\.gov\\.cn/\\w+/\\w+/\\w+/content.html";
    private static final String URL_POST4 = "http://www\\.miit\\.gov\\.cn/\\w+/\\w+/\\w+/\\w+/\\w+/content.html";
    //http://www.miit.gov.cn/n973401/n973642/n973647/c3847638/content.html
    //http://www.miit.gov.cn/n1146290/n1146402/n1146445/c5265066/content.html
    private static final String URL_POST5 = "http://www\\.miit\\.gov\\.cn/\\w[0-9]{6}/\\w[0-9]{6}/\\w[0-9]{6}/\\w[0-9]{7}/content.html";

    @Override
    public void process(Page page) {
        // 先要判断是否为空。然后再使用。否则出现异常情况时会报错。
        String id= MD5Util.GetMD5Code(page.getUrl().toString());
        if (id!=null)
        {
            page.putField("id", "cn.gov.miit.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
        }
        else
        {
            page.putField("id", "cn.gov.miit.www/");
        }
        page.putField("source_url", page.getUrl().toString());
        System.out.println(page.getUrl().toString());
        page.putField("crawler_time", DateUtil.getSystemCurrentDateTime());

       // page.putField("category", "动态");
        page.putField("source_content", page.getHtml().toString());
        page.putField("from_site", "工信部www.miit.gov.cn");
        if (page.getUrl().regex(URL_POST1).match() || page.getUrl().regex(URL_POST3).match()) {

            System.out.println("·························1或3```1或3```1或3```1或3```1或3······························");

//            String id= MD5Util.GetMD5Code(page.getUrl().toString());
//            if (id!=null)
//            {
//                page.putField("id", "cn.gov.miit.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
//            }
//            else
//            {
//                page.putField("id", "cn.gov.miit.www/");
//            }
//            page.putField("source_url", page.getUrl().toString());
//            System.out.println(page.getUrl().toString());
//            page.putField("crawler_time", DateUtil.getSystemCurrentDateTime());
//
//            // page.putField("category", "动态");
//            page.putField("source_content", page.getHtml().toString());
//            page.putField("from_site", "工信部www.miit.gov.cn");

            String title = page.getHtml().xpath("//h1[@id='con_title']/text()").toString();
            page.putField("title", title);
            String source = page.getHtml().xpath("//div[@class='cinfo center']/span[2]/text()").toString();
            System.out.println(source);
            String publishTime = page.getHtml().xpath("//span[@id='con_time']/text()").toString();
            if(publishTime==null||publishTime.equals("")){
                publishTime=page.getHtml()
                        .xpath("//table/tbody/tr[2]/td/table/tbody/tr/td[2]/text()")
                        .toString();
                source=page.getHtml()
                        .xpath("//table/tbody/tr[2]/td/table/tbody/tr/td[1]/text()")
                        .toString();

                if(publishTime!=null&&!"".equals(publishTime)){
                    publishTime=publishTime.replace("发布时间：","")
                            .replace("年","-").replace("月","-").replace("日","-") + " 00:00:00";
                    source=source.replace("文章来源：","").replace(" 来源：","");
                }else{
                    publishTime=page.getHtml()
                            .xpath("//div[@class='long_none wryh12black30']/div[@class='short_r']/text()")
                            .toString();
                    source = page.getHtml()
                            .xpath("//div[@class='long_gray wryh12black30']/div[@class='short_l']/text()")
                            .toString();

                    if(publishTime!=null&&!"".equals(publishTime)){
                        publishTime=publishTime.replace("发布时间：","") + " 00:00:00";
                        source=source.replace("文章来源：","").replace(" 来源：","");
                    }else{
                        publishTime="";
                        source="工业和信息化部";
                    }
                }

            }else{
                publishTime=publishTime.replace("发布时间：","") + " 00:00:00";
                source=source.replace("文章来源：","").replace(" 来源：","");

            }
            //System.out.println("发布时间1："+publishTime);
            page.putField("publish_time", publishTime);
            page.putField("source", source);
            page.putField("author", source);
            //page.putField("digest", "");

            String content = page.getHtml().xpath("//[@id='con_con']")
                    //.replace("../../../../../../", "http://www.miit.gov.cn/")
                    .toString();
            String cleanContent = CleanTextUtil.getCleanText(content);
            page.putField("cleanContent", cleanContent);
            try {
                SimHash simHash = new SimHash(cleanContent, 64);
                page.putField("simhash",simHash.get64strSimHash());
            } catch (IOException e) {
                e.printStackTrace();
            }
            ArrayList<String> segments = HanlpUtil.instance.getSegment(cleanContent);
            StringBuilder sb = new StringBuilder();
            for (String segment : segments) {
                sb.append(segment).append(" ");
            }
            page.putField("chineseSegment", sb.toString());

            String imageStr = page.getHtml().xpath("//div[@id='con_con']//img/@src").all().toString();
            String[] imageArr = imageStr.replace("[", "").replace("]", "").split(",");
            int imalength=imageArr.length;
            if(imageArr[imageArr.length-1]==null||imageArr[imageArr.length-1].equals("")){
                imalength=0;
            }
            for (int i = 0; i < imalength; i++) {
                String img_one = imageArr[i].trim();
                //int positon = Arrays.binarySearch(imageArr, img_one);
                content = content.replace(img_one, "images[" + i + "]");
            }
            content=content.replace("../../../../../../", "http://www.miit.gov.cn/");
            page.putField("content", ContentUtil.removeTags(content));

            //原网站的分类标签
            String sourceCate = page.getHtml().xpath("//div[@class='w980 center mnav']/span/a[5]/text()").all().toString();
            page.putField("source_tag", sourceCate);
            page.putField("images", page.getHtml().xpath("//div[@id='con_con']//img/@src").replace("../../../../../../", "http://www.miit.gov.cn/").all().toString());

            //附件
            //取最后一个P标签$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a/@href")
            List<String> attachmentlist =page.getHtml().$("div#con_con.ccontent.center ").xpath("//a/@href").all();
            //System.out.println("·········"+ page.getHtml().$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a").all());
            List<String> maplist = new ArrayList<String>();
            int i=1;
            if(attachmentlist.size()>0){
                String[] ss;
                for(String link: attachmentlist){
                    ss=link.split("\\.");
                    int l=ss.length;
                    System.out.println("length:"+l);
                    if(ss[l-1].equals("doc")||ss[l-1].equals("docx")
                            ||ss[l-1].equals("pdf")||ss[l-1].equals("txt")
                            ||ss[l-1].equals("xls")||ss[l-1].equals("jpg")){
                        String s="{\"title\":\"附件"+i+"\",\"link\":"+"\""+link+"\"}";
                        maplist.add(s);
                        System.out.println("可以");
                        i++;
                    }else {
                        continue;
                    }

                }
            }

            System.out.println(maplist);
            page.putField("attachments",maplist.toString());

            page.putField("category", ArticleCategoryUtil.instance.getCategory( title ));
            page.putField("digest", HanlpUtil.instance.getPartWordsSummary(CleanTextUtil.getCleanText(content)));
            ArrayList<String> sourceTagList=new ArrayList<String>();
            int number=10;
            ArrayList<String> keywords= KeywordExtractUtil.getKeyword(CleanTextUtil.getCleanText(content),sourceTagList,number);
            HashSet<String> sets = new HashSet<String>();
            for (String keyword : keywords) {
                sets.add(keyword);
            }
            StringBuilder setSb = new StringBuilder();
            for (String set: sets) {
                setSb.append(set).append(" ");
            }
            page.putField( "tag",setSb.toString().trim());


        } else if (page.getUrl().regex(URL_POST2).match()) {

            System.out.println("·························2```2```2```2```2```2```2```2······························");

//            String id= MD5Util.GetMD5Code(page.getUrl().toString());
//            if (id!=null)
//            {
//                page.putField("id", "cn.gov.miit.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
//            }
//            else
//            {
//                page.putField("id", "cn.gov.miit.www/");
//            }
//            page.putField("source_url", page.getUrl().toString());
//            System.out.println(page.getUrl().toString());
//            page.putField("crawler_time", DateUtil.getSystemCurrentDateTime());

            // page.putField("category", "动态");
            page.putField("source_content", page.getHtml().toString());
            page.putField("from_site", "工信部www.miit.gov.cn");

            String title = page.getHtml().xpath("//h2[@class='wryh18blueb']/text()").toString();
            page.putField("title", title);
            String sourceStr = page.getHtml().xpath("//div[@class='center time wryh14gray']/text()").toString();
            if (!StringUtil.isNullorBlank(sourceStr)) {
                String[] sourceArr = sourceStr.split("　　 文章来源：");
                String publishTime = sourceArr[0].replace("发布时间：", "");
                System.out.println(publishTime + "ddd");
                String source = sourceArr[1];
                page.putField("publish_time", publishTime + " 00:00:00");
                page.putField("source", source);
                page.putField("author", source);
            }
//            page.putField("publish_time", "工信部");
//            page.putField("source", "工");
//            page.putField("author", "工信部");
           // page.putField("digest", "");

            String content = page.getHtml().xpath("//div[@class='content']//p").all().toString();
            //去掉所有html标签的干净文本
            String cleanContent = CleanTextUtil.getCleanText(content);
            page.putField("cleanContent", cleanContent);
            try {
                SimHash simHash = new SimHash(cleanContent, 64);
                page.putField("simhash",simHash.get64strSimHash());
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
            int imalength=imageArr.length;
            if(imageArr[imageArr.length-1]==null||imageArr[imageArr.length-1].equals("")){
                imalength=0;
            }
            for (int i = 0; i < imalength; i++) {
                String img_one = imageArr[i].trim();
                //int positon = Arrays.binarySearch(imageArr, img_one);
                content = content.replace(img_one, "images[" + i + "]");
            }

            //System.out.println(content);
            page.putField("content", ContentUtil.removeTags(content));

            //原网站的分类标签
            String sourceCate = page.getHtml().xpath("//p[@class='wryh12white fl']").toString();
            if (!StringUtil.isNullorBlank(sourceCate)) {
                String[] arr = sourceCate.split("&gt;&nbsp;");
                page.putField("source_tag", arr[1].trim());
            }else {
                String sourceCate1 = page.getHtml().xpath("//div[@class='w980 center mnav']/span/a[5]/text()").all().toString();
                if (sourceCate1 != null) {
                    page.putField("source_tag",sourceCate1);
                }else {
                    String sourceCate2 = page.getHtml().xpath("//div[@class='w980 center mnav']/span/a[3]/text()").all().toString();
                    if(sourceCate2!=null){
                        page.putField("source_tag",sourceCate2);
                    }else{
                        page.putField("source_tag","[]");
                    }
                }
            }


            String  images = page.getHtml().xpath("//div[@class='content']//img/@src").toString();
            if(null == images){
                page.putField("images", "[]");
            }else{
                page.putField("images", page.getHtml().xpath("//div[@class='content']//img/@src").toString());
            }

            //附件
            //取最后一个P标签$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a/@href")
            List<String> attachmentlist =page.getHtml().$("div.content ").xpath("//a/@href").all();
            //System.out.println("·········"+ page.getHtml().$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a").all());
            List<String> maplist = new ArrayList<String>();
            int i=1;
            if(attachmentlist.size()>0){
                String[] ss;
                for(String link: attachmentlist){
                    ss=link.split("\\.");
                    int l=ss.length;
                    System.out.println("length:"+l);
                    if(ss[l-1].equals("doc")||ss[l-1].equals("docx")
                            ||ss[l-1].equals("pdf")||ss[l-1].equals("txt")
                            ||ss[l-1].equals("xls")||ss[l-1].equals("jpg")){
                        String s="{\"title\":\"附件"+i+"\",\"link\":"+"\""+link+"\"}";
                        maplist.add(s);
                        System.out.println("可以");
                        i++;
                    }else {
                        continue;
                    }

                }
            }

            System.out.println(maplist);
            page.putField("attachments",maplist.toString());
            page.putField("category", ArticleCategoryUtil.instance.getCategory( title ));

            page.putField("digest", HanlpUtil.instance.getPartWordsSummary(CleanTextUtil.getCleanText(content)));

            ArrayList<String> sourceTagList=new ArrayList<String>();
            int number=10;
            ArrayList<String> keywords= KeywordExtractUtil.getKeyword(CleanTextUtil.getCleanText(content),sourceTagList,number);
            HashSet<String> sets = new HashSet<String>();
            for (String keyword : keywords) {
                sets.add(keyword);
            }
            StringBuilder setSb = new StringBuilder();
            for (String set: sets) {
                setSb.append(set).append(" ");
            }
            page.putField( "tag",setSb.toString().trim());
        } else if (page.getUrl().regex(URL_POST4).match()) {

            System.out.println("·························4```4```4```4```4```4```4```4······························");

//            String id= MD5Util.GetMD5Code(page.getUrl().toString());
//            if (id!=null)
//            {
//                page.putField("id", "cn.gov.miit.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
//            }
//            else
//            {
//                page.putField("id", "cn.gov.miit.www/");
//            }
//            page.putField("source_url", page.getUrl().toString());
//            System.out.println(page.getUrl().toString());
//            page.putField("crawler_time", DateUtil.getSystemCurrentDateTime());

            // page.putField("category", "动态");
            page.putField("source_content", page.getHtml().toString());
            page.putField("from_site", "工信部www.miit.gov.cn");

            String title = page.getHtml().xpath("//h1[@id='con_title']/text()").toString();
            page.putField("title", title);
            String source = page.getHtml().xpath("/html/body/div[3]/div[7]/div[1]/text()").toString();
            String publishTime = page.getHtml()
                    .xpath("/html/body/div[3]/div[6]/div[2]/text()").toString();
            //System.out.println("#发布时间4："+publishTime);
            if(publishTime==null||publishTime.equals("")){
                publishTime=page.getHtml()
                        .xpath("//div[@class='cinfo center']/span[1]/text()").toString();
                source=page.getHtml()
                        .xpath("//div[@class='cinfo center']/span[2]/text()").toString();
                if(publishTime!=null&&!publishTime.equals("")){
                    publishTime=publishTime.replace("发布时间：","").replace("发布日期：","")+ " 00:00:00";
                    source=source.replace("文章来源：","").replace("来源：","");
                }else{
                    publishTime="";
                    source="工业和信息化部";
                }

            }else{
                publishTime=publishTime.replace("发布时间：","").replace("发布日期：","") + " 00:00:00";
                source=source.replace("文章来源：","").replace("来源：","");
            }
            //System.out.println("%发布时间4："+publishTime);
            page.putField("publish_time", publishTime);
            page.putField("source", source);
            String author = page.getHtml().xpath("/html/body/div[3]/div[3]/div[2]/text()").toString();
            if(author == null){
                page.putField("author", source);
            }else{
                page.putField("author", page.getHtml().xpath("/html/body/div[3]/div[3]/div[2]/text()").toString());
            }

           // page.putField("digest", "");

            String content = page.getHtml().xpath("//div[@id='con_con']").toString();
            String cleanContent = CleanTextUtil.getCleanText(content);
            page.putField("cleanContent", cleanContent);
            try {
                SimHash simHash = new SimHash(cleanContent, 64);
                page.putField("simhash",simHash.get64strSimHash());
            } catch (IOException e) {
                e.printStackTrace();
            }
            ArrayList<String> segments = HanlpUtil.instance.getSegment(cleanContent);
            StringBuilder sb = new StringBuilder();
            for (String segment : segments) {
                sb.append(segment).append(" ");
            }
            page.putField("chineseSegment", sb.toString());
            String imageStr = page.getHtml().xpath("//div[@id='con_con']//img/@src").all().toString();
            String[] imageArr = imageStr.replace("[", "").replace("]", "").split(",");
            int imalength=imageArr.length;
            if(imageArr[imageArr.length-1]==null||imageArr[imageArr.length-1].equals("")){
                imalength=0;
            }
            for (int i = 0; i < imalength; i++) {
                String img_one = imageArr[i].trim();
                //int positon = Arrays.binarySearch(imageArr, img_one);
                content = content.replace(img_one, "images[" + i + "]");
            }
            content=content.replace("../../../../../../", "http://www.miit.gov.cn/");
            page.putField("content", ContentUtil.removeTags(content));

            //原网站的分类标签
            String sourceCate = page.getHtml().xpath("/html/body/div[3]/div[7]/div[2]/text()").toString();
            if(sourceCate==null||sourceCate.equals("")){
                sourceCate="新闻";
            }else{
                sourceCate=sourceCate.replace("分　　类：", "");
            }
            page.putField("source_tag", sourceCate);
            page.putField("images", page.getHtml().xpath("//div[@id='con_con']//img/@src").replace("../../../../../../", "http://www.miit.gov.cn/").all().toString());

            //附件
            //取最后一个P标签$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a/@href")
            List<String> attachmentlist =page.getHtml().$("div#con_con.ccontent.center ").xpath("//a/@href").all();
            //System.out.println("·········"+ page.getHtml().$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a").all());
            List<String> maplist = new ArrayList<String>();
            int i=1;
            if(attachmentlist.size()>0){
                String[] ss;
                for(String link: attachmentlist){
                    ss=link.split("\\.");
                    int l=ss.length;
                    System.out.println("length:"+l);
                    if(ss[l-1].equals("doc")||ss[l-1].equals("docx")
                            ||ss[l-1].equals("pdf")||ss[l-1].equals("txt")
                            ||ss[l-1].equals("xls")||ss[l-1].equals("jpg")){
                        String s="{\"title\":\"附件"+i+"\",\"link\":"+"\""+link+"\"}";
                        maplist.add(s);
                        System.out.println("可以");
                        i++;
                    }else {
                        continue;
                    }

                }
            }

            System.out.println(maplist);
            page.putField("attachments",maplist.toString());

            page.putField("category", ArticleCategoryUtil.instance.getCategory( title ));
            page.putField("digest", HanlpUtil.instance.getPartWordsSummary(cleanContent));
            ArrayList<String> sourceTagList=new ArrayList<String>();
            int number=10;
            ArrayList<String> keywords= KeywordExtractUtil.getKeyword(cleanContent,sourceTagList,number);
            HashSet<String> sets = new HashSet<String>();
            for (String keyword : keywords) {
                sets.add(keyword);
            }
            StringBuilder setSb = new StringBuilder();
            for (String set: sets) {
                setSb.append(set).append(" ");
            }
            page.putField( "tag",setSb.toString().trim());

        } else if (page.getUrl().regex(URL_POST5).match()) {

            System.out.println("·························5```5```5```5```5```5```5```5······························");
//
//            String id= MD5Util.GetMD5Code(page.getUrl().toString());
//            if (id!=null)
//            {
//                page.putField("id", "cn.gov.miit.www/" + MD5Util.GetMD5Code(page.getUrl().toString()));
//            }
//            else
//            {
//                page.putField("id", "cn.gov.miit.www/");
//            }
//            page.putField("source_url", page.getUrl().toString());
//            System.out.println(page.getUrl().toString());
//            page.putField("crawler_time", DateUtil.getSystemCurrentDateTime());

            // page.putField("category", "动态");
            page.putField("source_content", page.getHtml().toString());
            page.putField("from_site", "工信部www.miit.gov.cn");

            String title = page.getHtml().xpath("/html/body/table[3]/tbody/tr/td/table[2]/tbody/tr[2]/td/table/tbody/tr/td/table[1]/tbody/tr[1]/td/span/text()").toString();
            page.putField("title", title);
            String publishStr = page.getHtml().xpath("/html/body/table[3]/tbody/tr/td/table[2]/tbody/tr[2]/td/table/tbody/tr/td/table[2]/tbody/tr/td/div/text()").toString();
            if (!StringUtil.isNullorBlank(publishStr)) {
                String publishTime = publishStr.trim().substring(0, 10);
                System.out.println(publishTime + "ddd");
                page.putField("publish_time", publishTime + " 00:00:00");
            }

            page.putField("source", "工信部");
            page.putField("author", "工信部");
            //page.putField("digest", "");

            String content = page.getHtml().xpath("/html/body/table[3]/tbody/tr/td/table[2]/tbody/tr[2]/td/table/tbody/tr/td/table[4]").toString();
            if(content==null||content.equals("")){
                content = page.getHtml().xpath("/html/body/table[4]/tbody/tr/td/table").all().toString();

                String cleanContent = CleanTextUtil.getCleanText(content);
                page.putField("cleanContent", cleanContent);
                try {
                    SimHash simHash = new SimHash(cleanContent, 64);
                    page.putField("simhash",simHash.get64strSimHash());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ArrayList<String> segments = HanlpUtil.instance.getSegment(cleanContent);
                StringBuilder sb = new StringBuilder();
                for (String segment : segments) {
                    sb.append(segment).append(" ");
                }
                page.putField("chineseSegment", sb.toString());

                String imageStr = page.getHtml().xpath("/html/body/table[4]/tbody/tr/td/table").all().toString();
                String[] imageArr = imageStr.replace("[", "").replace("]", "").split(",");
                int imalength=imageArr.length;
                if(imageArr[imageArr.length-1]==null||imageArr[imageArr.length-1].equals("")){
                    imalength=0;
                }
                for (int i = 0; i < imalength; i++) {
                    String img_one = imageArr[i].trim();
                    //int positon = Arrays.binarySearch(imageArr, img_one);
                    content = content.replace(img_one, "images[" + i + "]");
                }
                content=content.replace("../../../../../../", "http://www.miit.gov.cn/");
                page.putField("content", ContentUtil.removeTags(content));

            }else{
                String cleanContent = CleanTextUtil.getCleanText(content);
                page.putField("cleanContent", cleanContent);
                try {
                    SimHash simHash = new SimHash(cleanContent, 64);
                    page.putField("simhash",simHash.get64strSimHash());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ArrayList<String> segments = HanlpUtil.instance.getSegment(cleanContent);
                StringBuilder sb = new StringBuilder();
                for (String segment : segments) {
                    sb.append(segment).append(" ");
                }
                page.putField("chineseSegment", sb.toString());

                String imageStr = page.getHtml().xpath("/html/body/table[3]/tbody/tr/td/table[2]/tbody/tr[2]/td/table/tbody/tr/td/table[4]").all().toString();
                String[] imageArr = imageStr.replace("[", "").replace("]", "").split(",");
                int imalength=imageArr.length;
                if(imageArr[imageArr.length-1]==null||imageArr[imageArr.length-1].equals("")){
                    imalength=0;
                }
                for (int i = 0; i < imalength; i++) {
                    String img_one = imageArr[i].trim();
                    //int positon = Arrays.binarySearch(imageArr, img_one);
                    content = content.replace(img_one, "images[" + i + "]");
                }
                content=content.replace("../../../../../../", "http://www.miit.gov.cn/");
                page.putField("content", ContentUtil.removeTags(content));

            }

            //原网站的分类标签
            page.putField("source_tag", "动态");
            page.putField("images", page.getHtml().xpath("/html/body/table[3]/tbody/tr/td/table[2]/tbody/tr[2]/td/table/tbody/tr/td/table[4]").replace("../../../../../../", "http://www.miit.gov.cn/").all().toString());
            //附件
            //取最后一个P标签$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a/@href")
            List<String> attachmentlist =page.getHtml().$("table").xpath("//a/@href").all();
            //System.out.println("·········"+ page.getHtml().$("div#con_con.ccontent.center p:last-of-type").xpath("//p/a").all());
            List<String> maplist = new ArrayList<String>();
            int i=1;
            if(attachmentlist.size()>0){
                String[] ss;
                for(String link: attachmentlist){
                    ss=link.split("\\.");
                    int l=ss.length;
                    System.out.println("length:"+l);
                    if(ss[l-1].equals("doc")||ss[l-1].equals("docx")
                            ||ss[l-1].equals("pdf")||ss[l-1].equals("txt")
                            ||ss[l-1].equals("xls")||ss[l-1].equals("jpg")){
                        String s="{\"title\":\"附件"+i+"\",\"link\":"+"\""+link+"\"}";
                        maplist.add(s);
                        System.out.println("可以");
                        i++;
                    }else {
                        continue;
                    }

                }
            }

            System.out.println(maplist);
            page.putField("attachments",maplist.toString());
            page.putField("category", ArticleCategoryUtil.instance.getCategory( title ));
            page.putField("digest", HanlpUtil.instance.getPartWordsSummary(CleanTextUtil.getCleanText(content)));
            ArrayList<String> sourceTagList=new ArrayList<String>();
            int number=10;
            ArrayList<String> keywords= KeywordExtractUtil.getKeyword(CleanTextUtil.getCleanText(content),sourceTagList,number);
            HashSet<String> sets = new HashSet<String>();
            for (String keyword : keywords) {
                sets.add(keyword);
            }
            StringBuilder setSb = new StringBuilder();
            for (String set: sets) {
                setSb.append(set).append(" ");
            }
            page.putField( "tag",setSb.toString().trim());
        }
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        //customTime = args[0];
//       customTime = "2009-03-22";
        //读取配置文件的示例

        String collection= ParamsConfigurationUtil.instance.getParamString("mongodb.collection.MiitGov");

        //搜索请求链接
        String reqUrl = "http://searchweb.miit.gov.cn/search/search";
        //总页数
        int totalPage = 1;
        boolean iter=true;
//        while ()
        List<String> contentList = new ArrayList<String>();
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
        int pageNow=1;
        while(iter)
        {
            List<NameValuePair> qparams = new ArrayList<NameValuePair>();
            qparams.add(new BasicNameValuePair("name", "机器人"));
//            qparams.add(new BasicNameValuePair("fullText",""));
            qparams.add(new BasicNameValuePair("sortType", "1"));
            qparams.add(new BasicNameValuePair("indexDB", "css"));
            qparams.add(new BasicNameValuePair("sortFlag", "-1"));
            qparams.add(new BasicNameValuePair("pageNow", String.valueOf(pageNow)));
            qparams.add(new BasicNameValuePair("pageSize", "10"));
            qparams.add(new BasicNameValuePair("urls", "http://www.miit.gov.cn/"));
            qparams.add(new BasicNameValuePair("sortKey", "showTime"));
            String postRes = PostUtil.excutePost(reqUrl, qparams);
            //抓取的页面列表

            JSONObject json = JSONObject.fromObject(postRes);
            //System.out.println(postRes);
            JSONArray array = json.getJSONArray("array");

            for (int t = 0; t < array.size(); t++) {
                JSONObject jo = array.getJSONObject(t);
                String url = jo.getString("url");
                String showTime=jo.getString("showTime");
                contentList.add(url);

            }
            pageNow+=1;

        }
//            Spider.create(new MiitGovPage_bn())
//                    .startUrls(contentList)
//                    .addPipeline(new MongoDBPipeline(collection))
//                    //.addPipeline(new JsonFilePipeline("E://test/bigData/robotmiit"))
//                    .thread(1)
//                    .run();
        RedisSpiderCSIP.create(new MiitGovPage_bn()).startUrls(contentList).setDownloader(new RedisDownloaderCSIP())
                .run();



        //http://www.miit.gov.cn/n1146295/n1652858/n1652930/n3757016/c5168231/content.html
        //http://www.miit.gov.cn/n1146295/n1652858/n1652930/n4509650/c5183665/content.html

        /*Spider.create(new org.csip.bigData.crawler.service.MiitGovPage_bn())
                .addUrl("http://www.miit.gov.cn/n1146295/n1652858/n1652930/n4509650/c5183665/content.html")
                .addPipeline(new JsonFilePipeline("E:\\bigData_Project\\data\\"))
                //.addPipeline(new MongoDBPipeline(host, port, database, collection))
                .run();*/


    }

}
