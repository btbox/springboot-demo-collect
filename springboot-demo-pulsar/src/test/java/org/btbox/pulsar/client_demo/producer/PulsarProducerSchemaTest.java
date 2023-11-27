package org.btbox.pulsar.client_demo.producer;

import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.btbox.pulsar.pojo.User;
import org.junit.jupiter.api.Test;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_6650;

/**
 * @description: 生产者 schema 的方式方案
 * @author: BT-BOX
 * @createDate: 2023/11/24 16:36
 * @version: 1.0
 */
public class PulsarProducerSchemaTest {

    @Test
    @SneakyThrows
    public void send() {
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(SERVICE_HTTP_URL_6650).build();

        Producer<User> producer = pulsarClient.newProducer(AvroSchema.of(User.class))
                .topic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl2")
                .create();

        User user = new User();
        user.setName("btbox");
        user.setAge(20);
        user.setAddress("北京");

        producer.send(user);

        pulsarClient.close();

    }

}