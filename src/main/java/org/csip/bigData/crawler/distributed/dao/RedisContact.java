package org.csip.bigData.crawler.distributed.dao;

import redis.clients.jedis.Jedis;

/**
 * Created by bun@csip.org.cn on 2016/9/29.
 */
public class RedisContact {
    public void RedisContact() throws InterruptedException {
        Jedis jedis = new Jedis("54.223.50.219",6379);
        jedis.auth("csipBIG1692_pyjsJJHsm");
        jedis.rpush("userList","bn");
        String value=jedis.lpop("userList");
        jedis.setex("userList",5,"bn");
        System.out.println("value:"+value);
        Thread.sleep(5000);
        System.out.println("value:"+jedis.lpop("userList"));


    }
}
