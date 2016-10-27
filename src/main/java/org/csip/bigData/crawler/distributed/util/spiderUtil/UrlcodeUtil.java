package org.csip.bigData.crawler.distributed.util.spiderUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;

/**
 * UrlcodeUtil
 * @author caop
 *
 */
public class UrlcodeUtil {
	public static List<String> encoderStrList(List<String> urllist){
			for (int i = 0; i < urllist.size(); i++) {
				String url=urllist.get(i);
				String newurl="";
				for (int j = 0; j < url.length(); j++) {
				    if (isChinese(url.charAt(j))){
				    	newurl+=encoderStr(String.valueOf(url.charAt(j)));
				    }else{
				    	newurl+=url.charAt(j);
				    }
				    
				  }
				urllist.set(i, newurl);
		}
		return urllist;
	}

	public static String encodeSingleStr(String url) {
		String newurl = "";
		for (int j = 0; j < url.length(); j++) {
			if (isChinese(url.charAt(j))) {
				newurl += encoderStr(String.valueOf(url.charAt(j)));
			} else {
				newurl += url.charAt(j);
			}
		}
		return newurl;
	}

	public static String encoderStr(String url){
		try {
			url=URLEncoder.encode(url, "gbk");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return url;
	}
	/**
	 * 输入的字符是否是汉字
	 * @param a char
	 * @return boolean
	 */
	public static boolean isChinese(char a) { 
	     int v = (int)a; 
	     return (v >=19968 && v <= 171941); 
	}
	

	public static String decoderStr(String url){
		try {
			url=URLDecoder.decode(url, "gbk");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return url;
	}

}
