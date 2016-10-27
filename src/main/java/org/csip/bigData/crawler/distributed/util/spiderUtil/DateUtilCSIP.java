package org.csip.bigData.crawler.distributed.util.spiderUtil;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by bun@csip.org.cn on 2016/10/10.
 */
public class DateUtilCSIP {

    public static String currentDate(Date date)
    {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr=sdf.format(date);
        return dateStr;
    }
}
