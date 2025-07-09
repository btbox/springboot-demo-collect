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
 * @description:
 * @author: BT-BOX
 * @createDate: 2025/7/2 15:01
 * @version: 1.0
 */
public class SimpleTest {

    @Test
    @SneakyThrows
    public void producer1() {
        // 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("test-producer-group");
        // 连接namesrv
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 启动
        producer.start();
        // 创建一个消息
        Message message = new Message("testTopic", "我是一个简单的消息".getBytes());
        // 发送消息
        SendResult sendResult = producer.send(message);
        System.out.println(sendResult.getSendStatus());
        // 关闭生产者
        producer.shutdown();
    }

    @Test
    @SneakyThrows
    public void consumer1() {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("testTopic", "*");
        // 设置一个监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(new String(msgs.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }

}