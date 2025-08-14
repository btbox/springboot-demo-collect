// package org.btbox.rocketmq.consumer;
//
// import org.apache.rocketmq.common.message.MessageExt;
// import org.apache.rocketmq.spring.annotation.MessageModel;
// import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
// import org.apache.rocketmq.spring.core.RocketMQListener;
// import org.springframework.stereotype.Component;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2025/7/14 15:03
//  * @version: 1.0
//  */
// @Component
// @RocketMQMessageListener(
//         topic = "modelBroadcastTopic",
//         messageModel = MessageModel.BROADCASTING,      // 集群模式，负载均衡队列
//         consumerGroup = "model-broadcast-group-b"
// )
// public class BootB1BroadcastModelMsgListener implements RocketMQListener<MessageExt> {
//
//
//     @Override
//     public void onMessage(MessageExt message) {
//         System.out.println("我是model-cluster-group-b组的第一个消费者: " + new String(message.getBody()));
//     }
// }