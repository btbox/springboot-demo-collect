// package org.btbox.rocketmq.controller;
//
// import jakarta.annotation.Resource;
// import org.btbox.rocketmq.producer.RocketMQProducer;
// import org.springframework.web.bind.annotation.RequestMapping;
// import org.springframework.web.bind.annotation.RestController;
//
// @RestController
// @RequestMapping("/RocketMQ")
// public class RocketMQController {
//
//     private final String topic = "Test";
//
//     @Resource
//     private RocketMQProducer producer;
//
//     @RequestMapping("/sendMessage")
//     public String sendMessage(String message) {
//         producer.sendMessage(topic, message);
//         return "消息已发送";
//     }
//
// }
//
