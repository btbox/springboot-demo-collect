package org.btbox.pulsar.client_demo.producer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_6650;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/24 15:48
 * @version: 1.0
 */
@Slf4j
public class PulsarConsumerBatchTest {

    @Test
    @SneakyThrows
    public void consume() {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(SERVICE_HTTP_URL_6650).build();

        Consumer<String> consumer = pulsarClient.newConsumer(AvroSchema.STRING)
                .topic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl")
                .subscriptionName("sub_01")
                .batchReceivePolicy(BatchReceivePolicy.builder()
                        // 最大消息大小
                        .maxNumBytes(1024 * 1024)
                        // 每次接受消息最大数量
                        .maxNumMessages(5)
                        // 隔多长时间获取一次
                        .timeout(5, TimeUnit.SECONDS)
                        .build()
                )
                .subscribe();

        while (true) {
            // 批量接受消息
            Messages<String> messages = consumer.batchReceive();

            log.info("批量获取消息");

            for (Message<String> message : messages) {
                // 获取消息
                String msg = message.getValue();
                System.out.println("msg = " + msg);
                // ack 确认操作
                consumer.acknowledge(message);
            }
        }
    }

}