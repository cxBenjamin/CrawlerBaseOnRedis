package org.csip.bigData.crawler.distributed.util.dataProcessUtil;

import org.apache.commons.lang.StringUtils;
import org.csip.bigData.crawler.distributed.util.ParamsConfigurationUtil;

import java.util.ArrayList;

/**
 * Created by bun@csip.org.cn on 2016/9/22.
 */


//trends:动态，data：数据，observation:观察，policy:政策
public enum ArticleCategoryUtil {
    instance;
    private ArrayList<String> observationList;
    private  ArrayList<String> policyList;
    static
    {

        String observationFile = ParamsConfigurationUtil.instance.getParamString( "file.observations" );
        String policyFile = ParamsConfigurationUtil.instance.getParamString( "file.policy" );
        instance.observationList = ReadUserWordsFile.getUserDefinedKeywords( observationFile );
        instance.policyList= ReadUserWordsFile.getUserDefinedKeywords( policyFile );
    }

    public  String getCategory(String articleTitle) {
        ArrayList<String> words = HanlpUtil.instance.getSegment( articleTitle );
        boolean containsData = false;
        boolean containsObservation = false;
        boolean containsPolicy = false;
        for (String word : words) {
//            System.out.println( "word:" + word );
            if (StringUtils.isNumeric(word)) {
                containsData = true;

            }
            if (instance.observationList.contains(word)) {
                containsObservation = true;
//                System.out.println("观察");
            }
            if (instance.policyList.contains(word)) {
                containsPolicy = true;
//                System.out.println("政策");
            }

        }
        if (containsObservation) {
            return "观察";
        } else if (containsPolicy) {
            return "政策";
        } else if (containsData) {
//            System.out.println(observationList.size());
            return "数据";
        } else {
            return "动态";
        }


    }
}


