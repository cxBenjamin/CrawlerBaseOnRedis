package org.csip.bigData.crawler.distributed.util.dataProcessUtil;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Created by a on 2016/9/18.
 */
public class ContentUtil {

    public static String removeTags(String content) {



//                .replaceAll("<strong class[^>]*>", "")
//                .replaceAll("</strong>", "")
        Document document = Jsoup.parse(content);
        try {
            Elements elements = document.select("div[style=\"text-align: right; font-size: 12px;\"]");
            for (Element el : elements) {
                el.remove();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }



    content = document.body().toString().replaceAll("<a href[^>]*>", "")
            .replaceAll("</a>", "")
            .replaceAll("\\[|\\]", "")
            .replaceAll("\n|\r|\r\n","")
            .replaceAll("<body>","")
            .replaceAll("<a onclick[^>]*>", "");
//    public static void main(String[] args) {
//        System.out.println(ContentUtil.removeTags("sadfasfw\n\\uhv\r\r\neewaf"));
//    }
        return content;
    }
}
