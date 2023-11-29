package org.btbox.kafka.client.producer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.btbox.kafka.MyPartitioner;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.btbox.kafka.client.common.CommonConstant.CLUSTER_SERVICE_URL;

public class CustomProducerAcks {


    @SneakyThrows
    @Test
    void asyncSend() {
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER_SERVICE_URL);
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // acks level有 0 1 -1 级别描述看文档
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 1. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2. 发送数据
        for (int i = 0; i < 5; i++) {

            // 1. 限定分区,限定分区为2
            // new ProducerRecord<>("first", 2, "f", "btbox-" + i)

            // 2. 根据key来分区，将key:f的hash值和topic的partition的数进行取模得到partition的值
            // new ProducerRecord<>("first", "f", "btbox-" + i)

            // 3. 黏性分区器，随机选择一个分区，并尽可能一直使用该分区，待该分区的batch满了或者已完成，kafka会再随机一个分区进行使用(和上次的分区不同)
            // 例如: 第一次随机选择0号分区，等0号分区当前批次满了(默认16k)或者linger.ms设置的时间到，kafka会再随机一个分区进行使用(如果还是0会继续随机)
            producer.send(new ProducerRecord<>("first", "btbox-" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        System.out.println("主题: " + recordMetadata.topic() + "分区: " + recordMetadata.partition());
                    }
                }
            });

        }

        // 3. 关闭资源
        producer.close();
    }

}
