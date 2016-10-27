package org.csip.bigData.crawler.distributed.util;

import org.csip.bigData.crawler.distributed.util.spiderUtil.DateUtilCSIP;
import redis.clients.jedis.*;
import us.codecraft.webmagic.Request;

import java.util.*;

/**
 * Created by bun@csip.org.cn on 2016/10/3.
 */
public enum RedisUtilCSIP {
    instance;
//    protected JedisPool pool;
    protected Jedis jedis;



//    private static final String host=ParamsConfigurationUtil.instance.getParamString("redis.host");
//    private static final int port=ParamsConfigurationUtil.instance.getParamInteger("redis.port");
//    private static final String passwd=ParamsConfigurationUtil.instance.getParamString("redis.passwd");
    static{
        JedisPoolConfig config = new JedisPoolConfig();
        ResourceBundle bundle = ResourceBundle.getBundle("redis");
        String host = bundle.getString("redis.host");
        int port = Integer.valueOf(bundle.getString("redis.port"));
        int timeout = Integer.valueOf(bundle.getString("redis.timeout"));
        String passwd = bundle.getString("redis.auth");

//        config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
        config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
        config.setMinIdle(Integer.valueOf(bundle.getString("redis.pool.minIdle")));
//        config.setMaxWait(Integer.valueOf(bundle.getString("redis.pool.maxWait")));
        config.setMaxWaitMillis(Long.valueOf(bundle.getString("redis.pool.maxWait")));
//    config.setMaxTotal();
        config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
        config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn")));

    JedisPool pool=new JedisPool(new JedisPoolConfig(), host, port, timeout, passwd);
        instance.jedis=pool.getResource();
    //redis库的索引值，从0开始，默认16个库，数量可以在redis.conf配置文件中修改
        instance.jedis.select(0);


    }

//    public RedisUtilCSIP(final String host, int port, String passwd) {
//        this(new JedisPool(new JedisPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, passwd));
//        System.out.println("redis connect ok");
//    }
//
//    public RedisUtilCSIP(JedisPool pool) {
//        this.pool = pool;
//    }
    //存储到三种list中
    //FetchList
    //HistoryList
    //ExceptionList



    //网站待抓取内容页面链接列表，redis存储
//    针对一个网站的实例共享同一个Fetchlist
//3.2 在Fetchlist中保存待抓取的内容页面链接
//3.3 使用Redis List存储，作为Queue使用，遵循FIFO原则
//3.4 Fetchlist存储结构：
//    key: fetchlist:域名倒置
//    List：["页面完整链接地址"]


//    public void pushToFetchList(Request request, String ReverseDomain)
//    {
//        Jedis jedis=instance.pool.getResource();
//        String FetchListKey=RedisListName.FetchListName+ReverseDomain;
//        try
//        {
//
//            jedis.rpush(FetchListKey,request.getUrl());
////            jedis.set(Key,request.getUrl());
//        }
//        finally {
//            pool.returnResource(jedis);
//        }
//
//    }

    public void pushToFetchList(Request request)
    {

        String FetchListKey=RedisListName.FetchListName+URLUtil.getReverseDomain(request.getUrl());
//        try
//        {

            jedis.rpush(FetchListKey,request.getUrl());
            System.out.println("pushToFetchList");
//            jedis.set(Key,request.getUrl());
//        }
//        finally {
//            pool.returnResource(jedis);
//        }

    }



    //取得时候加上线程同步是为了防止两个线程争用url
    public synchronized  Request pollFromFetchList(String ReverseDomain)
    {
//        Jedis jedis=instance.pool.getResource();
        String FetchListKey=RedisListName.FetchListName+ReverseDomain;
        Request request=null;
//        try
//        {
            String url=jedis.lpop(FetchListKey);
//            if()
            //注意为空的情况。
            request=new Request(url);
//        }
//        finally {
//            pool.returnResource(jedis);
//        }
        return request;
    }
    public synchronized boolean isFetchListEmpty(Request request)
    {
//        Jedis jedis=instance.pool.getResource();
        String FetchListKey=RedisListName.FetchListName+URLUtil.getReverseDomain(request.getUrl());
//        boolean isFetchListEmpty
//        try
//        {
            long number=jedis.llen(FetchListKey);
            if (number>0.0)
            {
                return false;
            }
            else
            {
                return true;
            }
//        }
//        finally {
//            pool.returnResource(jedis);
//        }
    }


//4. Pagehistory，已进入爬虫视野的所有内容页链接，Redis存储
//4.1 保存所有来自同一网站的内容页信息
//    Key：pagehistory:域名倒置:页面完整地址MD5
//    Hashes：抓取状态（待抓取、正在抓取、已抓取、抓取异常、放弃抓取），发现时间帧（Timestamp, Long），页面完整地址
//4.2 对“正在抓取”状态的记录设置超时清除
//4.2.1 爬虫在抓取该链接时，程序因异常退出后，仍可以在重新上线后发现该链接，登记并抓取该链接内容

    //定义：待抓取： 正在抓取：  已抓取：  抓取异常：  放弃抓取：
    //fetchList的去重功能在pageHistoryList中实现。
    //返回值为true，说明key已经存在，false，说明key不存在，为新创建。
//    public synchronized boolean pushToPageHistoryHash(Request request,String CrawlerState,String ReverseDomain) {
//        Jedis jedis = instance.pool.getResource();
//        boolean isHit=false;
//        String PageHistoryKey = RedisListName.PageHistoryName + ReverseDomain+":"+ MD5Util.GetMD5Code(request.getUrl());
//
//        if (jedis.exists(PageHistoryKey)) {
//            isHit=true;
//        }
//            Map<String, String> valuesMap = new HashMap<String, String>();
//            //待抓取、正在抓取、已抓取、抓取异常、放弃抓取需要设置的。
//            valuesMap.put("URLCrawlState", CrawlerState);
//            valuesMap.put("Timestamp", new Date().toString());
//            valuesMap.put("Url", request.getUrl());
//            try {
//                jedis.hmset(PageHistoryKey, valuesMap);
//                System.out.println("pushToPageHistoryHash");
//
//            } finally {
//
//                pool.returnResource(jedis);
//
//            }
//
//            return isHit;
//    }




    public synchronized boolean pushToPageHistoryHash(Request request,String CrawlerState) {
//        Jedis jedis = instance.pool.getResource();
        boolean isHit=false;
        String PageHistoryKey = RedisListName.PageHistoryName + URLUtil.getReverseDomain(request.getUrl())+":"+ MD5Util.GetMD5Code(request.getUrl());

        if (jedis.exists(PageHistoryKey)) {
            isHit=true;
        }
        Map<String, String> valuesMap = new HashMap<String, String>();
        //待抓取、正在抓取、已抓取、抓取异常、放弃抓取需要设置的。
        valuesMap.put("URLCrawlState", CrawlerState);
        valuesMap.put("Timestamp", DateUtilCSIP.currentDate(new Date()));
        valuesMap.put("Url", request.getUrl());
//        try {
            jedis.hmset(PageHistoryKey, valuesMap);
            System.out.println("pushToPageHistoryHash");

//        } finally {
//
//            pool.returnResource(jedis);
//
//        }

        return isHit;
    }



    //废弃不用，有错
//    @Deprecated
//    public synchronized  Request pollFromPageHistoryHash(String key)
//    {
//        Request request=null;
////        Jedis jedis=instance.pool.getResource();
////        try {
//
//
////        }
////        finally {
////            {
////                pool.returnResource(jedis);
////            }
//        }
//        return request;
//    }

//    5. Exceptionlist，抓取时发生异常的内容页链接，Redis存储
//5.1 保存所有来自同一网站的发生抓取异常的内容页链接
//    Key: exceptionlist:域名倒置
//    Sorted Set: [score:"抓取尝试次数",value:"页面完整链接地址"]
//            5.2 抓取时发生解析异常时将链接加入（ZINCRBY）5.1的Sorted Set，且将4.1中该页面链接状态更新为“抓取异常”（使用WATCH MULTI，使用乐观锁执行事务操作）
//            5.3 程序实例可查询异常链接进行再次抓取，可设置多次抓取之后将记录从该列表中移除，并将pagehistory中对应记录置为“放弃抓取”状态


    //
//    public  void pushToExceptionSortedSet(Request request,String ReverseDomain)
//    {
//        Jedis jedis=instance.pool.getResource();
//        String ExceptionListKey=RedisListName.ExceptionSortedSetName+ReverseDomain;
//        String PageHistoryListKey=RedisListName.PageHistoryName+ReverseDomain+":"+ MD5Util.GetMD5Code(request.getUrl());
//        //初始值，以后不用设置了。
//        double score=1.0;
//        Map<String, String> valuesMap = new HashMap<String, String>();
//        //待抓取、正在抓取、已抓取、抓取异常、放弃抓取需要设置的。
//        valuesMap.put("URLCrawlState", URLCrawlState.Exception_Crawl);
//        valuesMap.put("Timestamp", new Date().toString());
//        valuesMap.put("Url", request.getUrl());
//
////暂时还不知道怎么用
////        jedis.watch(String.valueOf(score));
//        Transaction tx=jedis.multi();
//
//        try {
////            jedis.zadd(Key,score,request.getUrl());
//
//            tx.zincrby(ExceptionListKey,score,request.getUrl());
//            //不直接调用pushTohistoryList方法。
//            tx.hmset(PageHistoryListKey, valuesMap);
//            tx.exec();
//            System.out.println("pushToExceptionSortedSet");
//
//        }
//        finally {
//            pool.returnResource(jedis);
//        }
//
//    }


    public  void pushToExceptionSortedSet(Request request)
    {
        String ExceptionListKey=RedisListName.ExceptionSortedSetName+URLUtil.getReverseDomain(request.getUrl());
        String PageHistoryListKey=RedisListName.PageHistoryName+URLUtil.getReverseDomain(request.getUrl())+":"+ MD5Util.GetMD5Code(request.getUrl());
        String FetchListKey=RedisListName.FetchListName+URLUtil.getReverseDomain(request.getUrl());
        Map<String, String> valuesMap = new HashMap<String, String>();
        //待抓取、正在抓取、已抓取、抓取异常、放弃抓取需要设置的。
        valuesMap.put("URLCrawlState", URLCrawlState.Exception_Crawl);
        valuesMap.put("Timestamp", DateUtilCSIP.currentDate(new Date()));
        valuesMap.put("Url", request.getUrl());

        //暂时还不知道怎么用
        //        jedis.watch(String.valueOf(score));
        Transaction tx=jedis.multi();

        tx.zincrby(ExceptionListKey,1,request.getUrl());
            //不直接调用pushTohistoryList方法。
        tx.hmset(PageHistoryListKey, valuesMap);
        //应该再把这个url放入到fetchlist中。

        //现在看来不应该把这个url再放到FetchList中。
//        tx.rpush(FetchListKey,request.getUrl());
        tx.exec();
        System.out.println("pushToExceptionSortedSet");


    }


    //获取数据还有问题。 程序实例可查询异常链接进行再次抓取，可设置多次抓取之后将记录从该列表中移除，并将pagehistory中对应记录置为“放弃抓取”状态
    //准备采用事物的方法处理，
    //移除超过次数的链接，
    //在pagehistoryLIst中设置为“放弃抓取”状态。
   /////////////////////////////////
    ////////////////////
//    public synchronized  void pollFromExceptionSortedSet(Request request,String ReverseDomain)
//    {
//        Jedis jedis=instance.pool.getResource();
//        Transaction tx=jedis.multi();
//        String ExceptionListKey=RedisListName.ExceptionSortedSetName+ReverseDomain;
////        System.out.println("ExceptionListKey:"+ExceptionListKey);
////        Request request=null;
//        String PageHistoryListKey=RedisListName.PageHistoryName+ReverseDomain+":"+ MD5Util.GetMD5Code(request.getUrl());
//        Map<String, String> valuesMap = new HashMap<String, String>();
//        //待抓取、正在抓取、已抓取、抓取异常、放弃抓取需要设置的。
//        valuesMap.put("URLCrawlState", URLCrawlState.Givenup_Crawl);
//        valuesMap.put("Timestamp", new Date().toString());
//        valuesMap.put("Url", request.getUrl());
//        try{
//            //现在的问题是这个key对应的value是一个list，怎么具体删除其中的url呢?
//            tx.zrem(ExceptionListKey,request.getUrl());
//            tx.hmset(PageHistoryListKey, valuesMap);
//            tx.exec();
//        }
//        finally {
//            pool.returnResource(jedis);
//        }
////        return request;
//    }

    //设置入口参数，次数：score 到达这个次数就将这个url从exception中删除。
//    ,int countOut
    //加一个方法。将url的状态在pagehistoryList改为待抓取和重新加入到fetchList中。
    public synchronized void spiderCheckExceptionSortedSet(String ReverseDomain,int exceptionRetryTimes)
    {
        String ExceptionListKey=RedisListName.ExceptionSortedSetName+ReverseDomain;
//        Transaction tx=jedis.multi();
//        Response<Boolean> tcBool=tx.exists(ExceptionListKey);
        String cursor="0";
        double exceptionScore=exceptionRetryTimes*1.0;
        if (jedis.exists(ExceptionListKey)) {
//            Set<String> exceptionSet = jedis.zrange(ExceptionListKey, 0, -1);
//            //这段代码在分布式处理时，可能会出现问题。
//            for (String exceptionUrl : exceptionSet
//                    ) {
//                System.out.println(exceptionUrl);
//                Request request = new Request(exceptionUrl);
////状态改为待抓取。并且放入到FetchList中。
//                boolean isHit = pushToPageHistoryHash(request, URLCrawlState.NO_Crawl);
//                if (!isHit) {
//                    pushToFetchList(request);
//                }
//            }


            ScanResult<Tuple> tupleScanResult=jedis.zscan(ExceptionListKey,cursor);
            List<Tuple> tupleList=tupleScanResult.getResult();
            for (Tuple t:tupleList
                 ) {
                Request request = new Request(t.getElement());
                if(exceptionScore<=t.getScore())
                {
                    pollFromExceptionSortedSet(request);
                }
                else
                {
                    boolean isHit = pushToPageHistoryHash(request, URLCrawlState.NO_Crawl);
                if (!isHit) {
                    pushToFetchList(request);
                }
                }

            }


        }

    }



//    public synchronized void handleExceptionSortedSet(Request request)
//    {
//        String
//    }



    //还没有完全想明白。
    public synchronized  void pollFromExceptionSortedSet(Request request)
    {


        String ExceptionListKey=RedisListName.ExceptionSortedSetName+URLUtil.getReverseDomain(request.getUrl());
        String PageHistoryListKey=RedisListName.PageHistoryName+URLUtil.getReverseDomain(request.getUrl())+":"+ MD5Util.GetMD5Code(request.getUrl());
        Map<String, String> valuesMap = new HashMap<String, String>();

        valuesMap.put("URLCrawlState", URLCrawlState.Givenup_Crawl);
        valuesMap.put("Timestamp", DateUtilCSIP.currentDate(new Date()));
        valuesMap.put("Url", request.getUrl());
        Transaction tx=jedis.multi();
        tx.zrem(ExceptionListKey,request.getUrl());
        tx.hmset(PageHistoryListKey, valuesMap);
        tx.exec();
        System.out.println("pollFromExceptionSortedSet");

    }






    //通知记录
//    Key：notification:域名倒置
//    Hashes: {"lastNotifyTime":timestamp}
//    每次发生需要通知事件时，检查该记录不存在，发出通知，设置超时时间间隔，超时清除记录
    //暂时还不知道应该怎么做。
    public synchronized void pushToNotificationHash(String ReverseDomain)
    {

    }

}
