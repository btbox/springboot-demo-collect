package org.btbox.kafka.client.producer;

import lombok.Data;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.btbox.kafka.client.Student;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import static org.btbox.kafka.client.common.CommonConstant.CLUSTER_SERVICE_URL;

/**
 * 自定义对象序列化器
 */
public class CustomProducerSerializer {


    public static class StudentSerializer implements Serializer<Student> {

        @Override
        public byte[] serialize(String s, Student student) {
            ByteArrayOutputStream byteStream = null;
            ObjectOutputStream osStream = null;
            try {
                byteStream = new ByteArrayOutputStream();
                osStream = new ObjectOutputStream(byteStream);
                osStream.writeObject(student);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    byteStream.close();
                    osStream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return byteStream.toByteArray();
        }
    }

    @SneakyThrows
    @Test
    void asyncSend() {
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER_SERVICE_URL);
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StudentSerializer.class.getName());
        // acks level有 0 1 -1 级别描述看文档
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 1. 创建生产者对象
        KafkaProducer<String, Student> producer = new KafkaProducer<>(properties);

        // 2. 发送数据

        Student student = new Student();
        student.setName("btbox");
        student.setAge(19);
        producer.send(new ProducerRecord<>("topic_b", student), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (null == e) {
                    System.out.println("主题: " + recordMetadata.topic() + "分区: " + recordMetadata.partition());
                }
            }
        });


        // 3. 关闭资源
        producer.close();
    }


}
