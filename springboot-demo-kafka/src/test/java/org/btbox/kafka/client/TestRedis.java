package org.btbox.kafka.client;

import redis.clients.jedis.Jedis;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/6 15:06
 * @version: 1.0
 */
public class TestRedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost", 6379);
        jedis.set("name", "btbox");
        System.out.println(jedis.get("name"));
    }
}