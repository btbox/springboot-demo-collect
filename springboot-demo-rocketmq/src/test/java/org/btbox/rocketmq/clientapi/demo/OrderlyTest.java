package org.btbox.rocketmq.clientapi.demo;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.MessageQueue;
import org.btbox.rocketmq.domain.MsgModel;
import org.btbox.rocketmq.constants.MqConstant;
import org.junit.jupiter.api.Test;
import org.apache.rocketmq.common.message.Message;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @description: 顺序消费
 * @author: BT-BOX
 * @createDate: 2025/7/9 18:20
 * @version: 1.0
 */
public class OrderlyTest {

    private List<MsgModel> msgModels = Arrays.asList(
            new MsgModel("qwer", 1, "下单"),
            new MsgModel("qwer", 1, "短信"),
            new MsgModel("qwer", 1, "物流"),
            new MsgModel("zsxy", 2, "下单"),
            new MsgModel("zsxy", 2, "短信"),
            new MsgModel("zsxy", 2, "物流")

            );

    @Test
    public void orderlyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("orderly-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();


        msgModels.forEach(msgModel -> {
            Message message = new Message("orderlyTopic", msgModel.toString().getBytes());
            try {

                producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object arg) {
                        int hashCode = arg.toString().hashCode();
                        int i = hashCode % list.size();
                        return list.get(i);
                    }
                }, msgModel.getOrderSn());

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        producer.shutdown();
        System.out.println("发送成功");
    }

    @Test
    public void msConsumer() throws Exception {
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderly-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("orderlyTopic", "*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            System.out.println("线程id:" + Thread.currentThread().getId());
            System.out.println("收到消息了" + new Date());
            System.out.println(new String(msgs.get(0).getBody()));
            return ConsumeOrderlyStatus.SUCCESS;
        });
        // 启动
        consumer.start();
        // 挂起
        System.in.read();
    }

}