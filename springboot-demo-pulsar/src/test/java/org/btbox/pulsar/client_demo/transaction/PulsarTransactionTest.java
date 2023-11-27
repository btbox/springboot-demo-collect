package org.btbox.pulsar.client_demo.transaction;

import lombok.SneakyThrows;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_6650;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/27 11:29
 * @version: 1.0
 */
public class PulsarTransactionTest {

    @Test
    @SneakyThrows
    public void t() {
        // 1. 创建一个支持事务的Pulsar的客户端
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(SERVICE_HTTP_URL_6650).enableTransaction(true).build();

        // 2. 开启事务支持
        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES).build().get();

        try {

            // 3.

            // 3.1 接受消息数据
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic("txn_t1")
                    .subscriptionName("sub_txn")
                    .subscribe();

            Message<String> message = consumer.receive();

            // 3.2 将处理后的数据，发送另外一个Topic中
            Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic("txn_t2")
                    .create();

            producer.newMessage(transaction).value(message.getValue()).send();

            // 4. 确认消息
            consumer.acknowledge(message);

            // 5. 提交事务
            transaction.commit();
        } catch (Exception e) {
            transaction.abort();
        } finally {
            pulsarClient.close();
        }
    }

}