// package org.btbox.rocketmq.starter.v5;
//
// import jakarta.annotation.Resource;
// import lombok.SneakyThrows;
// import lombok.extern.slf4j.Slf4j;
// import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
// import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
// import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
// import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
// import org.apache.rocketmq.client.core.RocketMQClientTemplate;
// import org.apache.rocketmq.common.message.MessageExt;
// import org.btbox.rocketmq.constants.MqConstant;
// import org.junit.jupiter.api.Test;
// import org.springframework.boot.test.context.SpringBootTest;
// import org.springframework.messaging.Message;
// import org.springframework.messaging.support.MessageBuilder;
//
// import java.util.List;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2025/7/17 16:39
//  * @version: 1.0
//  */
// @Slf4j
// @SpringBootTest
// public class RocketMQV5Test {
//
//     @Resource
//     private RocketMQClientTemplate rocketMQClientTemplate;
//
//     @Test
//     public void test() {
//         Message<String> message = MessageBuilder.withPayload("我是boot2的一个消息").build();
//         rocketMQClientTemplate.send("v5BootTestTopic", message);
//     }
//
//     @Test
//     @SneakyThrows
//     public void consumer1() {
//         // 创建一个消费者
//         DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("v5-boot-consumer-group");
//         // 连接 namesrv
//         consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
//         // 订阅一个主题, * 标识订阅这个主题的所有消息
//         consumer.subscribe("v5BootTestTopic", "*");
//         // 设置一个监听器
//         consumer.registerMessageListener(new MessageListenerConcurrently() {
//             @Override
//             public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                 for (MessageExt msg : msgs) {
//                     log.info("消息：" + new String(msg.getBody()));
//                 }
//                 return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//             }
//         });
//         // 启动
//         consumer.start();
//         // 挂起
//         System.in.read();
//     }
//
// }