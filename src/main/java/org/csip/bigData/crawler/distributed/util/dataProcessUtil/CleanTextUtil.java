package org.csip.bigData.crawler.distributed.util.dataProcessUtil;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

/**
 * Created by bun@csip.org.cn on 2016/8/30.
 */
public class CleanTextUtil {
    private static Whitelist wlist = null;
    private static String mark1 = "&nbsp;";

    //可以去除掉绝大部分的html标签。初步获得一个干净的文本。
    public static String getCleanText(String html){
//        String text="";
        wlist= Whitelist.none();

        if(html==null){
            return "";
        }else{
            html = html.replace("[", "");
            html = html.replace("]", "");
            html = html.trim();
            String cleanText = Jsoup.clean(html, wlist);
            cleanText = cleanText.replaceAll(mark1, "");
            cleanText = cleanText.replaceAll("　　", "");
            return cleanText.trim();
        }
    }



}
