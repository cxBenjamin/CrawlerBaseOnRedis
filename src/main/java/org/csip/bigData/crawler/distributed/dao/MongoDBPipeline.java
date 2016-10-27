package org.csip.bigData.crawler.distributed.dao;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.utils.FilePersistentBase;

import java.util.Map;

public class MongoDBPipeline extends FilePersistentBase implements Pipeline {


    private String collention;

    public MongoDBPipeline(String collention) {

        this.collention = collention;
    }

    private MongoCollection collection=null;

    @Override
    public void process(ResultItems resultItems, Task task) {


        collection=MongoDBConnectUtil.instance.getCollection(collention);
        Map<String,Object> docMap=resultItems.getAll();

        Document doc =new Document();

        System.out.println("resultItem:"+resultItems.getAll().size());
        //去掉空的记录
        if(resultItems.getAll().size()>0) {
            for (Map.Entry<String, Object> entry : docMap.entrySet()) {

                doc.append(entry.getKey(), entry.getValue());

            }
        }
        //去掉空的记录。
        if(doc.size()>0) {
            //System.out.println("insert one----------- ");
            try {
                collection.insertOne(doc);
                System.out.println("insert one----------- ");
            }catch (Exception e)
            {
                System.out.println("存储失败！");
                e.printStackTrace();
            }
        }


    }
}
