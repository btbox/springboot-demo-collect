package org.btbox.kafka.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/4 10:50
 * @version: 1.0
 */
@RestController
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;


    @GetMapping("send")
    public void send(String msg) {
        kafkaTemplate.send("first", msg);
    }

}