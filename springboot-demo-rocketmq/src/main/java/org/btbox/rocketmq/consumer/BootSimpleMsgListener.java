// package org.btbox.rocketmq.consumer;
//
// import org.apache.rocketmq.common.message.MessageExt;
// import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
// import org.apache.rocketmq.spring.core.RocketMQListener;
// import org.springframework.stereotype.Component;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2025/7/14 11:12
//  * @version: 1.0
//  */
// @Component
// @RocketMQMessageListener(topic = "bootTestTopic", consumerGroup = "boot-test-consumer-group")
// public class BootSimpleMsgListener implements RocketMQListener<MessageExt> {
//
//     /**
//      *
//      * 如果泛型指定的是 MessageExt 则是消息的所有内容，其他类型这是 Message 的消息体
//      * -----------------
//      * 没有报错 就签收
//      * 报错就会拒收，就会 重试
//      * @param message
//      */
//     @Override
//     public void onMessage(MessageExt message) {
//         System.out.println(new String(message.getBody()));
//     }
//
// }