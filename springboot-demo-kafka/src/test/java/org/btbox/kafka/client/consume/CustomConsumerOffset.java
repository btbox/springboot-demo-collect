package org.btbox.kafka.client.consume;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.btbox.kafka.client.Student;
import org.btbox.kafka.client.common.CommonConstant;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.*;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/5 15:42
 * @version: 1.0
 */
public class CustomConsumerOffset {

    /**
     * 不提交offset
     */
    @Test
    public void noCommitOffset() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_group3");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 测试的时候注意，默认同一个消费组不允许重复消费，消费组存活时间默认是45秒，也就是45秒内再重启消费还是消费过了
        // 设置 SESSION_TIMEOUT_MS_CONFIG 存活时间为6秒，快速下架消费者
        pro.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
        // 提交offset策略，如果已经有offset则策略失效
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest 最后开始消费，none没有offset报异常,默认latest
        // 默认自动提交为true
        pro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_c");
        consumer.subscribe(topics);


        while (true) {
            ConsumerRecords<String, Student> records = consumer.poll(Duration.ofMillis(100));
            Iterator<ConsumerRecord<String, Student>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, Student> record = iterator.next();
                System.out.println(record.topic() + "-->" + record.partition() + "-->" + record.key() + "-->" + record.value() + "-->" + record.offset());
            }
            // 手动同步提交
            consumer.commitSync();
            // 手动异步提交
            consumer.commitAsync();
        }
    }

    /**
     * 手动提交offset
     */
    @Test
    public void manualCommit() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_offset_manual");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 测试的时候注意，默认同一个消费组不允许重复消费，消费组存活时间默认是45秒，也就是45秒内再重启消费还是消费过了
        // 设置 SESSION_TIMEOUT_MS_CONFIG 存活时间为6秒，快速下架消费者
        pro.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 关闭自动提交才能手动提交offset
        pro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_c");
        consumer.subscribe(topics);


        while (true) {
            ConsumerRecords<String, Student> records = consumer.poll(Duration.ofMillis(100));
            Iterator<ConsumerRecord<String, Student>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, Student> record = iterator.next();
                System.out.println(record.topic() + "-->" + record.partition() + "-->" + record.key() + "-->" + record.value() + "-->" + record.offset());
            }
            // 手动同步提交,提交失败则一直重试
            // consumer.commitSync();
            // 手动异步提交,只提交一次，不管是否失败，但是还是会记录最后一次提交成功的offset一直递增所以不怕失败，至少成功最后一次即可，大部分情况用这个
            consumer.commitAsync();
        }
    }

    /**
     * 指定偏移量消费
     */
    @Test
    public void seekCommit() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_offset_seek");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 测试的时候注意，默认同一个消费组不允许重复消费，消费组存活时间默认是45秒，也就是45秒内再重启消费还是消费过了
        // 设置 SESSION_TIMEOUT_MS_CONFIG 存活时间为6秒，快速下架消费者
        pro.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 关闭自动提交才能手动提交offset
        pro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_e");
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    // 指定每个分区都从 195 行开始消费
                    consumer.seek(partition, 195);
                }
            }
        });


        while (true) {
            ConsumerRecords<String, Student> records = consumer.poll(Duration.ofMillis(100));
            Iterator<ConsumerRecord<String, Student>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, Student> record = iterator.next();
                System.out.println(record.topic() + "-->" + record.partition() + "-->" + record.key() + "-->" + record.value() + "-->" + record.offset());
            }
            // 手动同步提交,提交失败则一直重试
            // consumer.commitSync();
            // 手动异步提交,只提交一次，不管是否失败，但是还是会记录最后一次提交成功的offset一直递增所以不怕失败，至少成功最后一次即可，大部分情况用这个
            consumer.commitAsync();
        }
    }


    /**
     * 指定时间消费
     */
    @Test
    public void offsetTimeStamp() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_offset_seek");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 测试的时候注意，默认同一个消费组不允许重复消费，消费组存活时间默认是45秒，也就是45秒内再重启消费还是消费过了
        // 设置 SESSION_TIMEOUT_MS_CONFIG 存活时间为6秒，快速下架消费者
        pro.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 关闭自动提交才能手动提交offset
        pro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_e");
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                Map<TopicPartition, Long> timeMap = new HashMap<>();
                for (TopicPartition partition : partitions) {
                    timeMap.put(partition, 1701835207374L);
                }
                // 指定时间找到偏移量
                Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timeMap);
                // 再根据偏移量信息找到数据并指定开始位置消费
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
                    if (null != entry.getValue()) {
                        System.out.println("entry.getKey().partition() = " + entry.getKey().partition());
                        System.out.println("entry.getValue().offset() = " + entry.getValue().offset());
                        consumer.seek(entry.getKey(), entry.getValue().offset());
                    }
                }
            }
        });


        while (true) {
            ConsumerRecords<String, Student> records = consumer.poll(Duration.ofMillis(100));
            Iterator<ConsumerRecord<String, Student>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, Student> record = iterator.next();
                System.out.println(record.topic() + "-->" + record.partition() + "-->" + record.key() + "-->" + record.value() + "-->" + record.offset());
            }
            // 手动同步提交,提交失败则一直重试
            // consumer.commitSync();
            // 手动异步提交,只提交一次，不管是否失败，但是还是会记录最后一次提交成功的offset一直递增所以不怕失败，至少成功最后一次即可，大部分情况用这个
            consumer.commitAsync();
        }
    }
}