package org.btbox.rocketmq.clientapi.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.btbox.rocketmq.constants.MqConstant;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;

/**
 * @description: 延迟消息
 * @author: BT-BOX
 * @createDate: 2025/7/9 15:06
 * @version: 1.0
 */
public class MsTest {

    @Test
    public void msProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ms-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();

        Message message = new Message("orderMsTopic", "订单号，座位号".getBytes());
        // 设置一个延迟时间
        message.setDelayTimeMs(10000);
        // 延迟级别
        // message.setDelayTimeLevel(4);
        producer.send(message);
        System.out.println("发送时间" + new Date());
        producer.shutdown();
    }

    @Test
    public void msConsumer() throws Exception {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ms-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("orderMsTopic", "*");
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