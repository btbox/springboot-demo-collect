package org.btbox.kafka.client.producer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.btbox.kafka.client.common.CommonConstant.CLUSTER_SERVICE_URL;

public class CustomProducerSync {



    @SneakyThrows
    @Test
    void syncSend() {
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER_SERVICE_URL);
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 1. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2. 发送数据
        for (int i = 0; i < 5; i++) {
            // get() 同步发送
            producer.send(new ProducerRecord<>("topic_c", "btbox-" + i)).get();
        }

        // 3. 关闭资源
        producer.close();
    }


}
