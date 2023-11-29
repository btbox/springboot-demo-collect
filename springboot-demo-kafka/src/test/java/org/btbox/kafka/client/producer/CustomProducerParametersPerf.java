package org.btbox.kafka.client.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.btbox.kafka.client.common.CommonConstant.CLUSTER_SERVICE_URL;

public class CustomProducerParametersPerf {


    @Test
    void send() {
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER_SERVICE_URL);
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // perf: 缓冲区大小  最好64M 67108864, 32M默认 33554432
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // perf: 批次大小 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // perf: linger.ms 多长时间发送一次到kafka存储 1ms, 1ms~100ms最佳
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);


        // perf: 压缩数据
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        // 1. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2. 发送数据
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "btbox-parameters" + i));
        }

        // 3. 关闭资源
        producer.close();
    }

}
