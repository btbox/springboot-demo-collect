package org.btbox.rocketmq.clientapi.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.btbox.rocketmq.constants.MqConstant;

import java.util.List;

public class RocketMQConsumer {
    public static void main(String[] args) throws Exception {
        // 初始化消费者（代码省略...）
        // 创建一个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-dead-consumer-group");
        // 连接 namesrv
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题, * 标识订阅这个主题的所有消息
        consumer.subscribe("%DLQ%retry-consumer-group", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
            for (MessageExt message : list) {
                // 获取消息的重试次数
                int reconsumeTimes = message.getReconsumeTimes();
                System.out.println("消息重试次数: " + reconsumeTimes);
                
                // 根据重试次数决定是否继续重试或进入死信队列
                if (reconsumeTimes >= 3) {
                    // 超过最大重试次数，处理失败或进入死信队列
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; // 或手动处理死信
                } else {
                    // 消费失败，RocketMQ 会自动重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
    }
}
