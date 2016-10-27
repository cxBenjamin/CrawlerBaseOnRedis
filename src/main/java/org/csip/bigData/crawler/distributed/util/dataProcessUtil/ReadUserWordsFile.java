package org.csip.bigData.crawler.distributed.util.dataProcessUtil;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by bun@csip.org.cn on 2016/9/22.
 */
public class ReadUserWordsFile {
    public static ArrayList<String> getUserDefinedKeywords(String fileName)
    {
        //"/config/params.properties"
        String filePath=null;
        ArrayList<String> userDefinedkeywordList=null;
        File file=null;
        try {
            fileName=System.getProperty("user.dir")+fileName;
            file=new File(fileName);
            userDefinedkeywordList=new ArrayList();
            LineIterator lineIterator = FileUtils.lineIterator(file);
            while(lineIterator.hasNext())
            {
                userDefinedkeywordList.add(lineIterator.nextLine().trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userDefinedkeywordList;
    }
}
