package org.btbox.pulsar.consume;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.btbox.pulsar.pojo.User;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Component;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/27 16:34
 * @version: 1.0
 */
@Component
public class HelloConsume {

    // @PulsarListener(subscriptionName = "hello-pulsar-sub", topics = "persistent://btbox_pulsar_t/btbox_pulsar_n/t_user1")
    // public void listen(User message) {
    //     System.out.println("listen: " + message);
    // }

    @PulsarListener(
            subscriptionName = "hello-pulsar-sub-batch",
            topics = "persistent://btbox_pulsar_t/btbox_pulsar_n/t_user1",
            batch = true
    )
    public void listen2(Messages<User> messages, Consumer<String> consumer) {
        System.out.println("批量监听:" + consumer.getConsumerName());
        for (Message<User> message : messages) {
            System.out.println(message.getValue());
        }
    }

}