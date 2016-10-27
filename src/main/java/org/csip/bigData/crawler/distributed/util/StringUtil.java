package org.csip.bigData.crawler.distributed.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字符串工具类
 */
public final class StringUtil {


    public static String reverseDomain(String Domain)
    {
        StringBuilder str=new StringBuilder(  );
        String[] strList=Domain.split("\\.");
        System.out.println(strList.length);
        for (int i = (strList.length-1); i>0 ; i--) {
//            System.out.println(strList[i]);
            str.append( strList[i]+"." );

        }
        str.append(strList[0]);
        return str.toString();
    }

    /**
     * 字符串是否为空:null或者长度为0.
     *
     * @param astr 源字符串.
     * @return boolean
     */
    public static boolean isBlank(String astr) {
        return ((null == astr) || (astr.length() == 0));
    }

    /**
     * 判断字符串为null或者为" "
     *
     * @param value 要判断的字符串
     * @return 是否为null或者为""
     */
    public static boolean isNullorBlank(String value) {
        return null == value || value.trim().isEmpty();
    }

    /**
     * 设置字符串编码为UTF-8
     *
     * @param str
     * @return
     */
    public static String encode(String str) {
        return encode(str, "UTF-8");
    }

    /**
     * 设置字符编码
     *
     * @param str
     * @param charset
     * @return
     */
    public static String encode(String str, String charset) {
        if (str != null && str.length() > 0) {
            try {
                return URLEncoder.encode(str, charset);
            } catch (UnsupportedEncodingException ex) {
                return str;
            }
        }
        return str;
    }

    /**
     * &#31215;&#20998;&#20817;&#25442;  > 积分兑换
     *
     * @param ASCIIs
     * @return
     */
    public static String ascii2String(String ASCIIs) {
        if (StringUtil.isNullorBlank(ASCIIs)) {
            return ASCIIs;
        }
        ASCIIs = ASCIIs.replaceAll(";&#", ",");
        ASCIIs = ASCIIs.replaceAll(";", "");
        ASCIIs = ASCIIs.replaceAll("&#", "");
        String[] ASCIIss = ASCIIs.split(",");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < ASCIIss.length; i++) {
            sb.append((char) ascii2Char(Integer.parseInt(ASCIIss[i])));
        }
        return sb.toString();
    }

    /**
     * 去掉指定字符串两端的空格
     *
     * @param value 指定的字符串
     * @return 去掉两端空格后的字符串。如果传入的指定字符串是null，返回""。
     */
    public static String trim(String value) {
        if (value == null) {
            return "";
        } else {
            return value.trim();
        }
    }

    /**
     * 转换成全角的方法。
     * <p>
     * 全角空格为12288，半角空格为32
     * <p>
     * 其他字符半角(33-126)与全角(65281-65374)的对应关系是：均相差65248
     *
     * @param input 要转换的字符串
     * @return 全角字符串
     */
    public static String ToSBC(String input) {

        char[] c = input.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == 32) {
                c[i] = (char) 12288;
                continue;
            }
            if (c[i] < 127)
                c[i] = (char) (c[i] + 65248);
        }
        return new String(c);
    }

    /**
     * 转换成半角的方法
     * <p>
     * 全角空格为12288，半角空格为32
     * <p>
     * 其他字符半角(33-126)与全角(65281-65374)的对应关系是：均相差65248
     *
     * @param input 要转换的字符串
     * @return 返回半角字符串
     */
    public static String ToDBC(String input) {
        char[] c = input.toCharArray();
        for (int i = 0; i < c.length; i++) {
            if (c[i] == 12288) {
                c[i] = (char) 32;
                continue;
            }
            if (c[i] > 65280 && c[i] < 65375)
                c[i] = (char) (c[i] - 65248);
        }
        return new String(c);
    }


    public static char ascii2Char(int ASCII) {
        return (char) ASCII;
    }

    public static String romoveEnter(String input) {
        return input.replaceAll("\r", "").replaceAll("\n", "");
    }

    public static Boolean isMobile(String mobile) {
        if (mobile == null || mobile.trim().length() != 11) {
            return false;
        }
        Pattern pattern = Pattern.compile("^(13[0-9]||15[012356789]||18[02356789]||147)\\d{8}$");
        Matcher matcher = pattern.matcher(mobile);
        if (matcher.matches()) {
            return true;
        }
        return false;
    }

    public static Boolean isPhone(String phone) {
        if (phone == null) {
            return false;
        }
        Pattern pattern = Pattern.compile("^0\\d{2,3}-?\\d{7,8}$");
        Matcher matcher = pattern.matcher(phone);
        if (matcher.matches()) {
            return true;
        }
        return false;
    }

    public static Boolean isEmail(String email) {
        if (StringUtil.isNullorBlank(email)) {
            return false;
        }
        Pattern pattern = Pattern.compile("^[0-9a-zA-Z\\.\\-_]+@([0-9a-z\\-_]*\\.*)([0-9a-z\\-_]+)\\.[a-z\\.]+$");
        Matcher mather = pattern.matcher(email);
        if (mather.matches()) {
            return true;
        }
        return false;
    }

    public static Boolean isId(String id) {
        if (StringUtil.isNullorBlank(id)) {
            return false;
        }
        return true;
    }

    public static Boolean isPayPassword(String payPassword) {
        if (StringUtil.isNullorBlank(payPassword)) {
            return false;
        }
        Pattern pattern = Pattern.compile("^([0-9a-zA-Z_]{6,16})$");
        Matcher mather = pattern.matcher(payPassword);
        if (mather.matches()) {
            return true;
        }
        return false;
    }

    public static boolean isNumber(String content) {
        if (StringUtil.isNullorBlank(content)) {
            return false;
        }
        content = content.trim();
        Pattern pattern = Pattern.compile("^[0-9]+$");
        Matcher matcher = pattern.matcher(content);
        if (matcher.matches()) {
            return true;
        }
        return false;
    }

    public static boolean isLetter(char c) {
        int k = 0x80;
        return c / k == 0 ? true : false;
    }

    /**
     * 得到一个字符串的长度,显示的长度,一个汉字或日韩文长度为2,英文字符长度为1
     *
     * @param s ,需要得到长度的字符串
     * @return int, 得到的字符串长度
     */
    public static int length(String s) {
        if (s == null)
            return 0;
        char[] c = s.toCharArray();
        int len = 0;
        for (int i = 0; i < c.length; i++) {
            len++;
            if (!isLetter(c[i])) {
                len++;
            }
        }
        return len;
    }

    public static String substring(String origin, int len) {
        if (origin == null || origin.equals("") || len < 1)
            return "";
        byte[] strByte = origin.getBytes();
        if (len > length(origin)) {
            return origin;
        }
        int count = 0;
        for (int i = 0; i < len; i++) {
            int value = (int) strByte[i];
            if (value < 0) {
                count++;
            }
        }
        if (count % 2 != 0) {
            len = (len == 1) ? ++len : --len;
        }
        return new String(strByte, 0, len);
    }

    /**
     * 分析字符串得到Integer.
     *
     * @param str1 String
     * @return Integer
     */
    public static Integer myparseIntObj(String str1) {
        try {
            if (isBlank(str1)) {
                return null;
            } else {
                // 16进制
                if (str1.startsWith("0x")) {
                    String sLast = str1.substring(2);
                    return Integer.valueOf(sLast, 16);
                } else {
                    return Integer.valueOf(str1);
                }
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 分析一个字符串,得到一个整数,如果错误,设置为缺省值-1.
     *
     * @param str1 String
     * @return int
     */
    public static int myparseInt(String str1) {
        return myparseInt(str1, -1);
    }

    /**
     * 分析一个字符串,得到一个整数,如果错误,设置为缺省值. 如果一个字符串以0x开头,则认为是16进制的.
     *
     * @param str1     字符串
     * @param nDefault 缺省值
     * @return int
     */
    public static int myparseInt(String str1, int nDefault) {
        int result;
        try {
            if (isBlank(str1)) {
                result = nDefault;
            } else {
                // 16进制
                if (str1.startsWith("0x")) {
                    String sLast = str1.substring(2);
                    result = Integer.parseInt(sLast, 16);
                } else {
                    result = Integer.parseInt(str1);
                }
            }
        } catch (NumberFormatException e) {
            result = nDefault;
        }
        return result;
    }

    /**
     * 分析一个字符串得到float,如果错误,设置一个缺省值-1.
     *
     * @param str1 String
     * @return float
     */
    public static float myparseFloat(String str1) {
        return myparseFloat(str1, -1);
    }

    /**
     * 分析一个字符串得到float,如果错误,设置一个缺省值.
     *
     * @param str1     String
     * @param nDefault 缺省值
     * @return float
     */
    public static float myparseFloat(String str1, float nDefault) {
        float result;
        try {
            result = isBlank(str1) ? nDefault : Float.parseFloat(str1);
        } catch (NumberFormatException e) {
            result = nDefault;
        }
        return result;
    }

    /**
     * 分析一个字符串得到Float,如果错误,返回null.
     *
     * @param str1 String
     * @return Float(may be null)
     */
    public static Float myparseFloatObj(String str1) {
        try {
            if (isBlank(str1)) {
                return null;
            } else {
                return Float.valueOf(str1);
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 分析一个字符串得到long,如果错误,设置一个缺省值 -1.
     *
     * @param str1 String
     * @return long
     */
    public static long myparseLong(String str1) {
        return myparseLong(str1, -1);
    }

    /**
     * 分析一个字符串得到long,如果错误,设置一个缺省值 .
     *
     * @param str1     字符串
     * @param nDefault 缺省值
     * @return long
     */
    public static long myparseLong(String str1, long nDefault) {
        long result;
        try {
            result = isBlank(str1) ? nDefault : Long.parseLong(str1);
        } catch (NumberFormatException e) {
            result = nDefault;
        }
        return result;
    }

    /**
     * 分析一个字符串得到Long,如果错误,返回null .
     *
     * @param str1 字符串
     * @return Long
     */
    public static Long myparseLongObj(String str1) {
        try {
            if (isBlank(str1)) {
                return null;
            } else {
                // 16进制
                if (str1.startsWith("0x")) {
                    String sLast = str1.substring(2);
                    return Long.valueOf(sLast, 16);
                } else {
                    return Long.valueOf(str1);
                }
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 去掉左右空格后字符串是否为空.
     *
     * @param astr String
     * @return boolean
     */
    public static boolean isTrimEmpty(String astr) {
        if ((null == astr) || (astr.length() == 0)) {
            return true;
        }
        if (isBlank(trim(astr))) {
            return true;
        }
        return false;
    }


}
