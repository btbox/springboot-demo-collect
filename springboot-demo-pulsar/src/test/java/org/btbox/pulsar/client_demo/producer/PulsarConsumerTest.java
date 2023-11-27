package org.btbox.pulsar.client_demo.producer;

import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.junit.jupiter.api.Test;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_6650;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/24 15:48
 * @version: 1.0
 */
public class PulsarConsumerTest {

    @Test
    @SneakyThrows
    public void consume() {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(SERVICE_HTTP_URL_6650).build();

        Consumer<String> consumer = pulsarClient.newConsumer(AvroSchema.STRING)
                .topic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl")
                .subscriptionName("sub_01")
                .subscribe();

        while (true) {
            // 接受消息
            Message<String> message = consumer.receive();
            // 获取消息
            String msg = message.getValue();
            System.out.println("msg = " + msg);
            // ack 确认操作
            consumer.acknowledge(message);

            // 如果消费失败了，可以采用try catch捕获异常，进行告知没有消费
            // consumer.negativeAcknowledge(message);
        }

        // consumer.close();
        // pulsarClient.close();

    }

}