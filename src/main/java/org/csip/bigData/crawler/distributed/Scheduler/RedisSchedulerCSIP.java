package org.csip.bigData.crawler.distributed.Scheduler;

import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.scheduler.DuplicateRemovedScheduler;
import us.codecraft.webmagic.scheduler.component.DuplicateRemover;

/**
 * Created by bun@csip.org.cn on 2016/9/29.
 */
public class RedisSchedulerCSIP extends DuplicateRemovedScheduler implements DuplicateRemover {



    public RedisSchedulerCSIP() {
        super();
    }

    @Override
    public DuplicateRemover getDuplicateRemover() {
        return super.getDuplicateRemover();
    }

    @Override
    public DuplicateRemovedScheduler setDuplicateRemover(DuplicateRemover duplicatedRemover) {
        return super.setDuplicateRemover(duplicatedRemover);
    }

    @Override
    public void push(Request request, Task task) {
        super.push(request, task);
    }

    @Override
    protected boolean shouldReserved(Request request) {
        return super.shouldReserved(request);
    }


    @Override
    protected void pushWhenNoDuplicate(Request request, Task task) {



    }

    public boolean isDuplicate(Request request, Task task) {
        return false;
    }

    @Override
    public void resetDuplicateCheck(Task task) {

    }


    public int getTotalRequestsCount(Task task) {
        return 0;
    }

    public Request poll(Task task) {
        return null;
    }
}
