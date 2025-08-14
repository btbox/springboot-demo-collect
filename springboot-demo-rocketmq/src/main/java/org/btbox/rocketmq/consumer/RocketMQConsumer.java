// package org.btbox.rocketmq.consumer;
//
// import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
// import org.apache.rocketmq.spring.core.RocketMQListener;
// import org.springframework.stereotype.Component;
//
// @Component
// @RocketMQMessageListener(consumerGroup = "consumer-test-group", topic = "Test")
// public class RocketMQConsumer implements RocketMQListener {
//
//     @Override
//     public void onMessage(Object message) {
//         System.out.println("Received message : " + message);
//     }
// }
//
