package org.btbox.kafka.client.producer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.btbox.kafka.client.common.CommonConstant.CLUSTER_SERVICE_URL;

public class CustomProducerTranactions {


    @SneakyThrows
    @Test
    void send() {
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER_SERVICE_URL);
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 设置事务id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tranactional_id_01");

        // 1. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 初始化事务
        producer.initTransactions();

        // 开始事务
        producer.beginTransaction();

        try {
            // 2. 发送数据
            for (int i = 0; i < 5; i++) {

                producer.send(new ProducerRecord<>("first", "btbox-tranactional" + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (null == e) {
                            System.out.println("主题: " + recordMetadata.topic() + "分区: " + recordMetadata.partition());
                        }
                    }
                });
            }
            // 测试异常回滚
            // int i = 1/0;
            producer.commitTransaction();
        } catch (Exception e) {
            // 出现异常事务回滚
            producer.abortTransaction();
        }

        // 3. 关闭资源
        producer.close();
    }

}
