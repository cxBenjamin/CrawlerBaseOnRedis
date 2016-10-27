package org.csip.bigData.crawler.distributed;

import org.apache.commons.collections.CollectionUtils;
import org.csip.bigData.crawler.distributed.util.RedisUtilCSIP;
import org.csip.bigData.crawler.distributed.util.StringUtil;
import org.csip.bigData.crawler.distributed.util.URLCrawlState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.*;
import us.codecraft.webmagic.downloader.Downloader;
import us.codecraft.webmagic.downloader.HttpClientDownloader;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.thread.CountableThreadPool;
import us.codecraft.webmagic.utils.UrlUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;

/**
 * Created by bun@csip.org.cn on 2016/10/3.
 */
//不实现还不明白的task，改造scheduler。
public class RedisSpiderCSIP implements Runnable,Task {
    protected Downloader downloader;

    protected List<Pipeline> pipelines = new ArrayList<Pipeline>();

    protected PageProcessor pageProcessor;

    protected List<Request> startRequests;

    protected Site site;

    protected String uuid;

//    protected RedisUtilCSIP scheduler = new RedisUtilCSIP("54.223.50.219",6379,"csipBIG1692_pyjsJJHsm");

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected CountableThreadPool threadPool;

    protected ExecutorService executorService;

    protected int threadNum = 1;

    protected AtomicInteger stat = new AtomicInteger(STAT_INIT);

    protected boolean exitWhenComplete = true;
    //线程未启动时为0，启动的时候为1，停止的时候为2
    protected final static int STAT_INIT = 0;

    protected final static int STAT_RUNNING = 1;

    protected final static int STAT_STOPPED = 2;

    protected boolean spawnUrl = true;

    protected boolean destroyWhenExit = true;

    private ReentrantLock newUrlLock = new ReentrantLock();

    private Condition newUrlCondition = newUrlLock.newCondition();

    private List<SpiderListener> spiderListeners;

    private final AtomicLong pageCount = new AtomicLong(0);

    private Date startTime;

    private int emptySleepTime = 30000;

    public static RedisSpiderCSIP create(PageProcessor pageProcessor)
    {
        return new RedisSpiderCSIP(pageProcessor);
    }


    public RedisSpiderCSIP(PageProcessor pageProcessor)
    {
        this.pageProcessor=pageProcessor;
        this.site=pageProcessor.getSite();
        this.startRequests=pageProcessor.getSite().getStartRequests();
    }

    public RedisSpiderCSIP startUrls(List<String> startUrls)
    {
        checkIfRunning();
        this.startRequests = UrlUtils.convertToRequests(startUrls);
        return this;
    }
    public RedisSpiderCSIP startRequest(List<Request> startRequests) {
        checkIfRunning();
        this.startRequests = startRequests;
        return this;
    }
//    public Spider scheduler(Scheduler scheduler) {
//        return setScheduler(scheduler);
//    }

    public RedisSpiderCSIP pipeline(Pipeline pipeline) {
        return addPipeline(pipeline);
    }

    /**
     * add a pipeline for Spider
     *
     * @param pipeline pipeline
     * @return this
     * @see Pipeline
     * @since 0.2.1
     */
    public RedisSpiderCSIP addPipeline(Pipeline pipeline) {
        checkIfRunning();
        this.pipelines.add(pipeline);
        return this;
    }
    public RedisSpiderCSIP addUrl(String... urls) {
        for (String url : urls) {
            addRequest(new Request(url));
        }
        signalNewUrl();
        return this;
    }

    public RedisSpiderCSIP setPipelines(List<Pipeline> pipelines) {
        checkIfRunning();
        this.pipelines = pipelines;
        return this;
    }

    /**
     * clear the pipelines set
     *
     * @return this
     */
    public RedisSpiderCSIP clearPipeline() {
        pipelines = new ArrayList<Pipeline>();
        return this;
    }

    public RedisSpiderCSIP downloader(Downloader downloader) {
        return setDownloader(downloader);
    }

    /**
     * set the downloader of spider
     *
     * @param downloader downloader
     * @return this
     * @see Downloader
     */
    public RedisSpiderCSIP setDownloader(Downloader downloader) {
        checkIfRunning();
        this.downloader = downloader;
        return this;
    }


    protected void initComponent() {
        //网页下载方法，默认使用HttpClientDownloader
        if (downloader == null) {
            this.downloader = new HttpClientDownloader();
        }
        //默认输出到ConsolePipeline
        if (pipelines.isEmpty()) {
            pipelines.add(new ConsolePipeline());
        }
        //设置线程数目。
        downloader.setThread(threadNum);
        if (threadPool == null || threadPool.isShutdown()) {
            if (executorService != null && !executorService.isShutdown()) {
                threadPool = new CountableThreadPool(threadNum, executorService);
            } else {
                threadPool = new CountableThreadPool(threadNum);
            }
        }


        //爬虫实例每次启动时，首先查询Redis Exceptionlist
//        尝试抓取网站上次抓取异常时，留下的内容页链接
//        如果内容页链接抓取出现异常，执行8.7.2的过程


        RedisUtilCSIP.instance.spiderCheckExceptionSortedSet(StringUtil.reverseDomain(this.getSite().getDomain()),this.site.getRetryTimes());



        if (startRequests != null) {
            for (Request request : startRequests) {
                //初始url加入schedule里面。要在这里修改。


                ///////////////////////////////////////////////////////////////

//                //这里是否需要加锁呢？
                //这里目前先不见bool结果，是为了防止初始的URL已经存在于pagehistory中，但是fetchlist为空的情况。
                boolean isHit=RedisUtilCSIP.instance.pushToPageHistoryHash(request, URLCrawlState.NO_Crawl);
                //种子url也是待抓取url，放入到fetchList中。
                //待抓取       页面
                //这段代码的思路还没有理清楚。
//                boolean isEmpty=RedisUtilCSIP.instance.isFetchListEmpty(request);
                if(!isHit) {
                    RedisUtilCSIP.instance.pushToFetchList(request);
                }//待抓取： 正在抓取：  已抓取：  抓取异常：  放弃抓取：
//                RedisUtilCSIP.instance.pushToFetchList(request);
//                scheduler.push(request, this);
            }
            startRequests.clear();
        }
        startTime = new Date();
    }
    private void checkRunningStat() {
        while (true) {
            int statNow = stat.get();
//            stat.get();
            //startNow==1?
            //初始的StartNow应该为0
            if (statNow == STAT_RUNNING) {
                throw new IllegalStateException("Spider is already running!");
            }
            if (stat.compareAndSet(statNow, STAT_RUNNING)) {
                break;
            }
        }
    }

    public void close() {
        destroyEach(downloader);
        destroyEach(pageProcessor);
//        destroyEach(RedisUtilCSIP.instance);
        for (Pipeline pipeline : pipelines) {
            destroyEach(pipeline);
        }
        threadPool.shutdown();
    }

    private void destroyEach(Object object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    protected void checkIfRunning() {
        if (stat.get() == STAT_RUNNING) {
            throw new IllegalStateException("Spider is already running!");
        }
    }

    @Override
    public void run() {
        checkRunningStat();
        //上面的组件初始化。
        initComponent();
        //貌似UUID也就是记录在日志里，这个作用，还有写在pipeline中。
        logger.info("Spider " + " started!");
        while (!Thread.currentThread().isInterrupted() && stat.get() == STAT_RUNNING) {



//           //每次从FetchList中抓取一条url来。
            final Request request = RedisUtilCSIP.instance.pollFromFetchList(StringUtil.reverseDomain(this.site.getDomain()));
            /////////////////////////////////
            if (request.getUrl() == null) {
                if (threadPool.getThreadAlive() == 0 && exitWhenComplete) {
                    break;
                }
                // wait until new url added
                //等待有新的url的到来。
                waitNewUrl();
            } else {
                //为什么要赋值给一个final  上。
                final Request requestFinal = request;
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //处理url
                            processRequest(requestFinal);
                            ///////////////////////////////////
                            RedisUtilCSIP.instance.pushToPageHistoryHash(requestFinal, URLCrawlState.Haven_Crawl);

                            //待试验新的
                            onSuccess(requestFinal);
                        } catch (Exception e) {
                            RedisUtilCSIP.instance.pushToExceptionSortedSet(requestFinal);
                            onError(requestFinal);
                            logger.error("process request " + requestFinal + " error", e);
                        } finally {
                            pageCount.incrementAndGet();
                            signalNewUrl();
                        }
                    }
                });
            }
        }
        stat.set(STAT_STOPPED);
        // release some resources
        if (destroyWhenExit) {
            close();
        }
    }

    protected void onError(Request request) {
        if (CollectionUtils.isNotEmpty(spiderListeners)) {
            for (SpiderListener spiderListener : spiderListeners) {
                spiderListener.onError(request);
            }
        }
    }

    protected void onSuccess(Request request) {
        if (CollectionUtils.isNotEmpty(spiderListeners)) {
            for (SpiderListener spiderListener : spiderListeners) {
                spiderListener.onSuccess(request);
            }
        }
    }

    protected void extractAndAddRequests(Page page, boolean spawnUrl) {
        if (spawnUrl && CollectionUtils.isNotEmpty(page.getTargetRequests())) {
            for (Request request : page.getTargetRequests()) {
                addRequest(request);
            }
        }
    }

    private void addRequest(Request request) {
        if (site.getDomain() == null && request != null && request.getUrl() != null) {
            site.setDomain(UrlUtils.getDomain(request.getUrl()));
        }

        //scheduler添加新的url。
        ///////////////////////////////////////////////////////////////
        //种子url也是待抓取url，放入到fetchList中。
        boolean isHit=RedisUtilCSIP.instance.pushToPageHistoryHash(request, URLCrawlState.NO_Crawl);
        if(!isHit) {
            //不在history中才把这条url推到FetchList中。
            RedisUtilCSIP.instance.pushToFetchList(request);
        }



    }


    @Override
    public String getUUID() {
        if (uuid != null) {
            return uuid;
        }
        if (site != null) {
            return site.getDomain();
        }
        uuid = UUID.randomUUID().toString();
        return uuid;
    }

    @Override
    public Site getSite() {
        return site;
    }


    protected void processRequest(Request request) {
        //获得资源页面，这里主要是html页面。
        //问题是默认的downloader是在哪里加载的？     已经解决。
        //为什么参数是task要用this来代替？在这里貌似task并没有用到？



        ////////////////////////////////////////  null
        RedisUtilCSIP.instance.pushToPageHistoryHash(request,URLCrawlState.Being_Crawl);
        System.out.println("正在抓取");

        //注意这个方法，成功返回的是page，失败返回的是null。
        Page page = downloader.download(request,this);
//        RedisUtilCSIP.instance.pushToPageHistoryHash(request, URLCrawlState.Haven_Crawl);

        if (page == null) {

            //具体的出错信息写到这里。exceptionList中记录，pagehistoryList中记录为“记录异常”
            //exceptionList只是把出错的url记录，但是没有记录出错原因，比如说404,501等等。
//            RedisUtilCSIP.instance.pushToExceptionSortedSet(request);
//            System.out.println("pushToExceptionSortedSet：抓取异常");
            RedisUtilCSIP.instance.pushToPageHistoryHash(request, URLCrawlState.Exception_Crawl);
            RedisUtilCSIP.instance.pushToExceptionSortedSet(request);
            System.out.println("pushToExceptionSortedSet：抓取异常");
            throw new RuntimeException("unaccpetable response status");
        }
        else
        {
            //获取数据成功，写入到pagehistoryList中，注意抓取状态为“已抓取”
            RedisUtilCSIP.instance.pushToPageHistoryHash(request, URLCrawlState.Haven_Crawl);
            System.out.println("pagehistory:已经抓取     "+request.getUrl());

        }
        // for cycle retry
        //这里的重试时间，和site中的retrytimes有什么联系吗？







        //测试放在这里是否合适



//        RedisUtilCSIP.instance.pushToPageHistoryHash(request, URLCrawlState.Haven_Crawl);







        //这里的休眠重试，可以禁用。全部都交给redis处理。
        if (page.isNeedCycleRetry()) {
            extractAndAddRequests(page, true);

            //休眠重试的时间在这里定义呀。
            try {
                sleep(site.getRetrySleepTime());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }
        //页面处理程序。这里就是具体的页面解析规则的地方。
        pageProcessor.process(page);
        //从page中提取出新的url来
        extractAndAddRequests(page, spawnUrl);
        if (!page.getResultItems().isSkip()) {
            for (Pipeline pipeline : pipelines) {
                /////////////////////////////////////////    null

                //输出，实例化，在这里实现。
                pipeline.process(page.getResultItems(), null);
            }
        }
        //for proxy status management
        request.putExtra(Request.STATUS_CODE, page.getStatusCode());
        try {
            sleep(site.getSleepTime());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private void waitNewUrl() {
        //线程锁，
        newUrlLock.lock();
        try {
            //double check
            if (threadPool.getThreadAlive() == 0 && exitWhenComplete) {
                return;
            }
            newUrlCondition.await(emptySleepTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("waitNewUrl - interrupted, error {}", e);
        } finally {
            newUrlLock.unlock();
        }
    }
    private void signalNewUrl() {
        try {
            newUrlLock.lock();
            newUrlCondition.signalAll();
        } finally {
            newUrlLock.unlock();
        }
    }
}
