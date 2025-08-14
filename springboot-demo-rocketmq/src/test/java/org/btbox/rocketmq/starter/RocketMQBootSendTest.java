// package org.btbox.rocketmq.starter;
//
// import com.alibaba.fastjson2.JSON;
// import jakarta.annotation.Resource;
// import org.apache.rocketmq.client.producer.SendCallback;
// import org.apache.rocketmq.client.producer.SendResult;
// import org.apache.rocketmq.spring.core.RocketMQTemplate;
// import org.apache.rocketmq.spring.support.RocketMQHeaders;
// import org.btbox.rocketmq.domain.MsgModel;
// import org.junit.jupiter.api.Test;
// import org.springframework.boot.test.context.SpringBootTest;
// import org.springframework.messaging.Message;
// import org.springframework.messaging.support.MessageBuilder;
//
// import java.util.Arrays;
// import java.util.List;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2025/7/14 11:24
//  * @version: 1.0
//  */
// @SpringBootTest
// public class RocketMQBootSendTest {
//
//     @Resource
//     private RocketMQTemplate rocketMqTemplate;
//
//     @Test
//     public void send() {
//
//         rocketMqTemplate.syncSend("bootTestTopic", "我是boot的一个消息");
//
//         rocketMqTemplate.asyncSend("bootAsyncTestTopic", "我是boot的一个异步消息", new SendCallback() {
//             @Override
//             public void onSuccess(SendResult sendResult) {
//                 System.out.println("发送成功");
//             }
//
//             @Override
//             public void onException(Throwable throwable) {
//                 System.out.println("发送失败: " + throwable.getMessage());
//             }
//         });
//
//         // 单向
//         rocketMqTemplate.sendOneWay("bootOnewayTopic", "单向消息");
//
//         // 延迟
//         Message<String> message = MessageBuilder.withPayload("我是一个延迟消息").build();
//         rocketMqTemplate.syncSendDelayTimeSeconds("bootMsTopic", message, 3);
//
//         List<MsgModel> msgModels = Arrays.asList(
//                 new MsgModel("qwer", 1, "下单"),
//                 new MsgModel("qwer", 1, "短信"),
//                 new MsgModel("qwer", 1, "物流"),
//                 new MsgModel("zsxy", 2, "下单"),
//                 new MsgModel("zsxy", 2, "短信"),
//                 new MsgModel("zsxy", 2, "物流")
//         );
//
//         // 顺序发送
//         for (MsgModel msgModel : msgModels) {
//             rocketMqTemplate.syncSendOrderly("bootOrderlyTopic", JSON.toJSONString(msgModel), msgModel.getOrderSn());
//         }
//
//         // tag
//         rocketMqTemplate.syncSend("bootTagTopic:tagA", "我是一个带tag的消息");
//
//         // key是写带在消息头的
//         Message<String> keyMessage = MessageBuilder.withPayload("我是一个带key的消息").setHeader(RocketMQHeaders.KEYS, "qwertyuiop").build();
//         rocketMqTemplate.syncSend("bootKeyTopic", keyMessage);
//     }
//
//     /**
//      * 测试集群模式下的消息
//      */
//     @Test
//     public void sendTestModelCluster() {
//         for (int i = 0; i < 5; i++) {
//             rocketMqTemplate.syncSend("modelClusterTopic", "我是第" + i + "个消息");
//         }
//     }
//
//     @Test
//     public void sendTestModelBroadcast() {
//         for (int i = 0; i < 5; i++) {
//             rocketMqTemplate.syncSend("modelBroadcastTopic", "我是第" + i + "个消息");
//         }
//     }
//
//     @Test
//     public void sendTrace() {
//         rocketMqTemplate.syncSend("traceTopic", "我是第个消息");
//     }
//
//
// }