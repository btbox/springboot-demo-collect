package org.btbox.kafka.client.producer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.btbox.kafka.client.common.CommonConstant.CLUSTER_SERVICE_URL;

public class CustomProducerInterceptor {


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
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyInterceptor.class.getName());

        // 1. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2. 发送数据
        for (int i = 0; i < 5; i++) {

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


    public static class MyInterceptor implements ProducerInterceptor<String, String> {

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
            // 拦截数据并修改
            String msg = producerRecord.value();
            msg = System.currentTimeMillis() + "_" + msg;
            return new ProducerRecord<String, String>(producerRecord.topic(), msg);
        }

        @Override
        public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
            // 确认ack后
            if (null == e) {
                System.out.println("success-->" + recordMetadata.topic() + "-->" + recordMetadata.partition() + "-->" + recordMetadata.offset());
            } else {
                System.out.println("===========fail==========");
            }
        }

        @Override
        public void close() {
            // 关闭
        }

        @Override
        public void configure(Map<String, ?> map) {

        }
    }

}
