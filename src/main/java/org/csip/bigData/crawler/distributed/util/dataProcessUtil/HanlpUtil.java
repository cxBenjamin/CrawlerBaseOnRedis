package org.csip.bigData.crawler.distributed.util.dataProcessUtil;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.seg.common.Term;
import org.csip.bigData.crawler.distributed.util.ParamsConfigurationUtil;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by bun@csip.org.cn on 2016/9/14.
 */

//关键词提取相关的分词工具包
public enum HanlpUtil {
    instance;

    HanlpUtil()
    {
        String fileName= ParamsConfigurationUtil.instance.getParamString("file.keywords");
        ArrayList<String> userDefineWords=ReadUserWordsFile.getUserDefinedKeywords(fileName);
        for (String word:userDefineWords
             ) {
            CustomDictionary.add(word);

        }

    }


    public ArrayList<String> getSegment(String chineseText)
    {
        List<Term> termList=HanLP.segment(chineseText);
        ArrayList<String> wordList=new ArrayList<String>();
        for (Term term:termList
             ) {
            wordList.add(term.word);

        }
//        HanLP.
        return wordList;
    }

    //语义上的关键字提取，用的是hanlp自己的方法
    public ArrayList<String> getSematicKeyword(String chineseText,int keywordNumber)
    {
        ArrayList<String> keywordList= (ArrayList)HanLP.extractKeyword(chineseText,keywordNumber);
        return keywordList;
    }
//语义上的摘要提取，用的是hanlp自己的方法，提取的效果不是特别好。
    public List<String> getSematicSummary(String chineseText,int number)
    {
        List<String> summaryList=HanLP.extractSummary(chineseText,number);
        return summaryList;
    }


    //摘要，取前五分之一的单词作为摘要
    public String getPartWordsSummary(String chineseText)
    {
        if(chineseText.length()>=200){
            String summary = chineseText.substring(0, 200);
            return summary;
        }else{
            return chineseText;
        }
    }
}
