package org.btbox.rocketmq.clientapi.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.btbox.rocketmq.constants.MqConstant;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2025/7/9 15:26
 * @version: 1.0
 */
public class BatchTest {

    /**
     * 并发消息会发送到同一个 Queue
     * @throws Exception
     */
    @Test
    public void batchProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("batch-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();

        List<Message> messages = new ArrayList<>();
        messages.add(new Message("batchTopic", "订单号1".getBytes()));
        messages.add(new Message("batchTopic", "订单号2".getBytes()));
        messages.add(new Message("batchTopic", "订单号3".getBytes()));
        // 延迟级别
        // message.setDelayTimeLevel(4);
        producer.send(messages);
        producer.shutdown();
    }

    @Test
    public void batchConsumer() throws Exception {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("batch-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("batchTopic", "*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println("收到消息了" + new Date());
            System.out.println(new String(msgs.get(0).getBody()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }

}