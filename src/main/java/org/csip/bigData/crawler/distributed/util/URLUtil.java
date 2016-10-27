package org.csip.bigData.crawler.distributed.util;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by bun@csip.org.cn on 2016/10/8.
 */
public class URLUtil {
    private static URL url;
    public static String getReverseDomain(String urlStr)
    {
        String reverseDomain="";
        try {
            url=new URL(urlStr);
            reverseDomain=url.getHost();
            reverseDomain=StringUtil.reverseDomain(reverseDomain);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return reverseDomain;
    }
}
