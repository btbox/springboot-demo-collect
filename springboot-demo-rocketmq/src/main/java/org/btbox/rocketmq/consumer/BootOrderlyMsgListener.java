// package org.btbox.rocketmq.consumer;
//
// import com.alibaba.fastjson2.JSON;
// import org.apache.rocketmq.common.message.MessageExt;
// import org.apache.rocketmq.spring.annotation.ConsumeMode;
// import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
// import org.apache.rocketmq.spring.core.RocketMQListener;
// import org.btbox.rocketmq.domain.MsgModel;
// import org.springframework.stereotype.Component;
//
// /**
//  * @description: 顺序消费者
//  * @author: BT-BOX
//  * @createDate: 2025/7/14 11:12
//  * @version: 1.0
//  */
// @Component
// @RocketMQMessageListener(
//         topic = "bootOrderlyTopic",
//         consumerGroup = "boot-orderly-consumer-group",
//         consumeMode = ConsumeMode.ORDERLY,               // 顺序消费 单线程
//         maxReconsumeTimes = 5                            // 消费重试次数
// )
// public class BootOrderlyMsgListener implements RocketMQListener<MessageExt> {
//
//     /**
//      * 如果泛型指定的是 MessageExt 则是消息的所有内容，其他类型这是 Message 的消息体
//      * -----------------
//      * 没有报错 就签收
//      * 报错就会拒收，就会 重试
//      *
//      * @param message
//      */
//     @Override
//     public void onMessage(MessageExt message) {
//
//         MsgModel msgModel = JSON.parseObject(new String(message.getBody()), MsgModel.class);
//
//         System.out.println(msgModel);
//     }
//
// }