package org.csip.bigData.crawler.distributed.util.spiderUtil;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.List;

/**
 * HttpPost方法
 * Created by csip on 2016/8/4.
 */
public class PostUtil {
    //本意是轮换浏览器head。实际只用了第一个。
    public static String USER_AGENT[] = {
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586"
    };
    public static final String ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8";
    public static final String ACCEPT_LANGUAGE = "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3";
    public static final String ACCEPT_ENCODING = "gzip, deflate";
    public static final String CONNECTION = "keep-alive";

    /**
     * 提交post请求，返回需要抓取页面的url集合
     *
     * @param reqUrl  请求url
     * @param qparams 请求参数
     * 进行页面下载
     * @return
     */
    public static String excutePost(String reqUrl, List<NameValuePair> qparams) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        //String path = "/bigdata/post3/";
        //File writeFile = new File(path + "post.txt");
        String content = "";
        try {
            HttpResponse response = null;
            HttpPost httpPost = new HttpPost(reqUrl);
            //随机取浏览器代理
            httpPost.setHeader("User-Agent", UserAgentUtil.getUserAgent());
            httpPost.setHeader("Accept", ACCEPT);
            httpPost.setHeader("Accept-Language", ACCEPT_LANGUAGE);
            httpPost.setHeader("Accept-Encoding", ACCEPT_ENCODING);
            httpPost.setHeader("Connection", CONNECTION);
            httpPost.setEntity(new UrlEncodedFormEntity(qparams, "UTF-8"));
            response = httpClient.execute(httpPost);
            int stateCode=response.getStatusLine().getStatusCode();
            if(stateCode==403)
            {
                System.out.println("stateCode:%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" +stateCode);
            }
            HttpEntity httpEntity = response.getEntity();
            content = EntityUtils.toString(httpEntity);
            //FileUtils.writeStringToFile(writeFile, content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }
    public static String excuteGet(String reqUrl) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        //String path = "/bigdata/post3/";
        //File writeFile = new File(path + "post.txt");
        String content = "";
        HttpResponse response = null;
        try {

            HttpGet httpGet=new HttpGet(reqUrl);
            //随机取浏览器代理
            httpGet.setHeader("User-Agent", UserAgentUtil.getUserAgent());
            httpGet.setHeader("Accept", ACCEPT);
            httpGet.setHeader("Accept-Language", ACCEPT_LANGUAGE);
            httpGet.setHeader("Accept-Encoding", ACCEPT_ENCODING);
            httpGet.setHeader("Connection", CONNECTION);
            response = httpClient.execute(httpGet);
            int stateCode=response.getStatusLine().getStatusCode();
            if(stateCode==403)
            {
                System.out.println("stateCode:%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" +stateCode);
            }
            HttpEntity httpEntity = response.getEntity();
            content = EntityUtils.toString(httpEntity);
            //FileUtils.writeStringToFile(writeFile, content);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

}
