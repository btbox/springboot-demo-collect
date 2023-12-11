package org.btbox.kafka.controller;

import lombok.RequiredArgsConstructor;
import org.btbox.kafka.domain.entity.Person;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
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

    private final KafkaTemplate<String, Object> kafkaTemplate;


    @PostMapping("send")
    @Transactional
    public void send() {
        for (int i = 0; i < 10; i++) {
            Person person = new Person();
            person.setAge(i);
            person.setUsername("name-" + i);
            // 需要添加@Transactional否则报错
            // No transaction is in process; possible solutions: run the template operation within the scope of a template.executeInTransaction() operation, start a transaction with @Transactional before invoking the template method, run in a transaction started by a listener container when consuming a record
            kafkaTemplate.send("topic_f", person);
            // 测试异常事务是否自动回滚
            // int g = 1/0;
            // 第二种方式，如果不添加@Transactional
//             kafkaTemplate.executeInTransaction((KafkaOperations.OperationsCallback<String, Object, Object>) kafkaOperations -> {
//                 kafkaOperations.send("topic_f" , person);
//                 throw new RuntimeException("test transaction");
//                 return "";
//             });
        }

    }

    @PostMapping("send2")
    @Transactional
    public void send2() {
        for (int i = 0; i < 1; i++) {
            Person person = new Person();
            person.setAge(i);
            person.setUsername("name-" + i);
            // 需要添加@Transactional否则报错
            // No transaction is in process; possible solutions: run the template operation within the scope of a template.executeInTransaction() operation, start a transaction with @Transactional before invoking the template method, run in a transaction started by a listener container when consuming a record
            kafkaTemplate.send("topic-dlt-test", person);

        }

    }
}