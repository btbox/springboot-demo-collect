package org.btbox.rocketmq.controller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2025/8/6 17:57
 * @version: 1.0
 */
@Slf4j
@RestController
@RequestMapping("/V5RocketMQ")
public class V5RocketMQController {

    @Resource
    private RocketMQClientTemplate template;

    @RequestMapping("/sendMessage")
    public void sendMessage(String message) {
        SendReceipt sendReceipt = template.syncSendNormalMessage("normalTopic", message);
        log.info("普通消息发送完成：topic={},  message = {}, sendReceipt = {}", "v5BootTestTopic", message, sendReceipt);
    }

}