package org.btbox.kafka.client.consume;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.btbox.kafka.client.Student;
import org.btbox.kafka.client.common.CommonConstant;
import org.btbox.kafka.client.producer.CustomProducerSerializer;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/5 15:42
 * @version: 1.0
 */
public class CustomConsumerDeserializer {
    @Test
    public void poll() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_group3");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyStudentDeserializer.class.getName());

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_b");
        consumer.subscribe(topics);



        while (true) {
            ConsumerRecords<String, Student> records = consumer.poll(Duration.ofMillis(100));
            Iterator<ConsumerRecord<String, Student>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, Student> record = iterator.next();
                System.out.println(record.topic() + "-->" + record.partition() + "-->" + record.key() + "-->" + record.value() + "-->" + record.offset());
            }
        }
    }

    // public static class MyDeserializer implements Deserializer<String> {
    //
    //     @Override
    //     public String deserialize(String topic, byte[] bytes) {
    //         String line = new String(bytes, StandardCharsets.UTF_8);
    //         return line;
    //     }
    // }



    public static class MyStudentDeserializer implements Deserializer<Student> {

        @Override
        public Student deserialize(String s, byte[] bytes) {
            ByteArrayInputStream byteIn = null;
            ObjectInputStream objectIn = null;
            Student student = null;
            try {
                byteIn = new ByteArrayInputStream(bytes);
                objectIn = new ObjectInputStream(byteIn);
                Object o = objectIn.readObject();
                student = (Student) o;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    byteIn.close();
                    objectIn.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return student;
        }
    }

}