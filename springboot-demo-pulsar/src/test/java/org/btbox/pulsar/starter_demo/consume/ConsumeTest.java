package org.btbox.pulsar.starter_demo.consume;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.pulsar.annotation.PulsarListener;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/27 14:44
 * @version: 1.0
 */
@SpringBootTest
public class ConsumeTest {

    @Test
    @PulsarListener(subscriptionName = "hello-pulsar-sub", topics = "persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl1")
    public void listen(String message) {
        System.out.println("Message Received: " + message);
    }


}