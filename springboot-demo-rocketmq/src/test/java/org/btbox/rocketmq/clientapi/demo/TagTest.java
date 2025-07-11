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
import org.btbox.rocketmq.constants.MqConstant;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @description: tag
 * @author: BT-BOX
 * @createDate: 2025/7/11 10:18
 * @version: 1.0
 */
public class TagTest {

    @Test
    public void tagProducer() throws Exception {
        // 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("tag-producer-group");
        // 连接namesrv
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 启动
        producer.start();
        // 创建一个消息
        Message message = new Message("tagTopic", "vip1", "我是vip1的文章".getBytes());
        Message message2 = new Message("tagTopic", "vip2", "我是vip2的文章".getBytes());
        // 发送消息
        producer.send(message);
        producer.send(message2);
        // 关闭生产者
        producer.shutdown();
    }

    @Test
    @SneakyThrows
    public void tagConsumer1() {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-a");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("tagTopic", "vip1");
        // 设置一个监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("我是vip1的消防者，我正在消费消息" + new String(msgs.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }

    @Test
    @SneakyThrows
    public void tagConsumer2() {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-b");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("tagTopic", "vip1 || vip2");
        // 设置一个监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("我是vip2的消防者，我正在消费消息" + new String(msgs.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }

}