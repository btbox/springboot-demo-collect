// package org.btbox.rocketmq.consumer;
//
// import org.apache.rocketmq.common.message.MessageExt;
// import org.apache.rocketmq.spring.annotation.MessageModel;
// import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
// import org.apache.rocketmq.spring.core.RocketMQListener;
// import org.springframework.stereotype.Component;
//
// /**
//  * CLUSTERING 集群模式下，队列会被消费者分摊，队列数量>=消费者数量，消息的消费点位 mq服务器会记录处理
//  * BROADCASTING 广播模式下，消息会被每个消费者都处理一次，mq服务器不会记录消费点位，也不会重试
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2025/7/14 14:44
//  * @version: 1.0
//  */
// @Component
// @RocketMQMessageListener(
//         topic = "modelClusterTopic",
//         messageModel = MessageModel.CLUSTERING,      // 集群模式，负载均衡队列
//         consumerGroup = "model-cluster-group-a"
// )
// public class BootA2ClusterModelMsgListener implements RocketMQListener<MessageExt> {
//     @Override
//     public void onMessage(MessageExt message) {
//         System.out.println("我是model-cluster-group-a组的第二个消费者: " + new String(message.getBody()));
//     }
// }