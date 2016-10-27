package org.csip.bigData.crawler.distributed.util.dataProcessUtil;
import org.csip.bigData.crawler.distributed.util.ParamsConfigurationUtil;

import java.util.ArrayList;

/**
 * Created by bun@csip.org.cn on 2016/9/12.
 */
public class KeywordExtractUtil {



// （1）先用网站自己的tag
//    （2）从30个关键词
//     （3）自己语义分析
//            数量不受限制

    public static ArrayList<String> getKeyword(String content, ArrayList<String> sourceTagList,int sematicWordNumber)
    {

//分词
        ArrayList<String> segments= HanlpUtil.instance.getSegment(content);
        //加载30个关键词
        String fileName=ParamsConfigurationUtil.instance.getParamString("file.keywords");
        ArrayList<String> userDefines=ReadUserWordsFile.getUserDefinedKeywords(fileName);
        //从分词中找出30个关键词。
        segments.retainAll(userDefines);
        ArrayList<String> SematicKeywords= HanlpUtil.instance.getSematicKeyword(content,sematicWordNumber);
        SematicKeywords.removeAll(segments);
        segments.addAll(SematicKeywords);
        //如果网站自己有tag，加上
        if(sourceTagList.size()>0)
        {
            sourceTagList.removeAll(segments);
            segments.addAll(sourceTagList);
        }
        return segments;
    }

}
