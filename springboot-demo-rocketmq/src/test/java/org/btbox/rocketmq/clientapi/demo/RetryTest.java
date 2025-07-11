package org.btbox.rocketmq.clientapi.demo;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.btbox.rocketmq.constants.MqConstant;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @description: 重试消息
 * @author: BT-BOX
 * @createDate: 2025/7/11 15:42
 * @version: 1.0
 */
public class RetryTest {

    @Test
    public void retryProducer() throws Exception {
        // 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("retry-producer-group");
        // 连接namesrv
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 启动
        producer.start();

        // 生产者发送消息 重试次数
        producer.setRetryTimesWhenSendFailed(2);
        producer.setRetryTimesWhenSendAsyncFailed(2);
        String key = UUID.randomUUID().toString();
        System.out.println(key);

        // 创建一个消息
        Message message = new Message("retryTopic", "vip1", key, "vip1文章".getBytes());
        // 发送消息
        producer.send(message);
        System.out.println("发送成功");
        // 关闭生产者
        producer.shutdown();
    }

    /**
     * 不包含第一次消费，从第二次开始计算重试次数
     * 重试的时间间隔
     * 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * 默认重试16次 重试间隔为上面阶梯时间,若重试次数超过16次，后面每次重试间隔都为2小时。
     * 1. 能否自定义重试次数
     * 2. 如果重试16次(并发模式)顺序模式下(int最大值次就是20亿多)都是失败？是一个死信消息，则会放在一个死信队列: %DLQ%retry-consumer-group
     * 3. 当消息处理失败的时候应该怎么正确处理
     *
     * 重试次数一般设置5-7次
     */
    @Test
    @SneakyThrows
    public void consumer1() {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("retryTopic", "*");
        consumer.setMaxReconsumeTimes(2);
        // 设置一个监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(new Date());
                System.out.println(new String(msgs.get(0).getBody()));
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }


    @Test
    @SneakyThrows
    public void retryDeadConsumer() {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-dead-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("%DLQ%retry-consumer-group", "*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println(new Date());
            System.out.println("记录到特别的位置 文件或者数据库，通知人工处理: " + new String(msgs.get(0).getBody()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }


    /**
     * 比较好的解决方案
     * 直接判断重试次数，直接记录数据库或者其他地方，不需要进入到死信队列
     */
    @Test
    @SneakyThrows
    public void retryDeadConsumerFinal() {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-dead-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("retryTopic", "*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt message : msgs) {
                // 获取消息的重试次数
                int reconsumeTimes = message.getReconsumeTimes();
                System.out.println("消息重试次数: " + reconsumeTimes);


                try {
                    // 业务处理...

                } catch (Exception e) {
                    if (reconsumeTimes >= 3) {
                        // 超过最大重试次数，处理失败或进入死信队列
                        System.out.println("记录到特别的位置 文件或者数据库，通知人工处理: " + new String(message.getBody()));
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 或手动处理死信
                    } else {
                        // 消费失败，RocketMQ 会自动重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }

            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }

}