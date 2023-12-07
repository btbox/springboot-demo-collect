package org.btbox.kafka.client.consume;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.btbox.kafka.client.Student;
import org.btbox.kafka.client.common.CommonConstant;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/5 15:42
 * @version: 1.0
 */
public class CustomConsumerPartitionRule {

    /**
     * 范围分区分配策略,首先会计算每个consumer可以消费的分区个数，然后按照顺序将指定个数范围的分区分配给各个consumer
     * <p>
     * 弊端： Range针对单个Topic的情况下显得比较均衡， 但是假如Topic很多的话, consumer排序靠前的可能会比 consumer排序靠后的负载多很多。
     */
    @Test
    public void rangeAssignor() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_group3");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 默认 RangeAssignor 规则，随机分区并且第一个分的最多，例如5个分区，两个消费者的话，a消费者有3个分区，b有2个分区。
        pro.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_c");
        // ConsumerRebalanceListener 是分区监听器
        // 例如 topic_c 是5个分区，在起1个消费者的时候，全部分区都为改消费者，起2个以上的消费者开始重新分区，平衡每个消费者都有分区可以消费
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            // 回收之前的分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("revoke " + partition.topic() + "-->" + partition.partition());
                }
            }

            // 分配分区
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("assign " + partition.topic() + "-->" + partition.partition());
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
        }
    }




    /**
     * RoundRobin是针对所有topic分区。它是采用轮询分区策略，是把所有的partition、所有的consumer列举出来 进行排序，最后再通过轮询分配partition给每个消费者（如果该消费者没有订阅该主题则跳到下一个消费者）
     * <p>
     * 弊端：在Consumer Group有订阅消息不一致的情况下，我们最好不要选用RoundRobin
     */
    @Test
    public void roundRobinAssignor() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_group3");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        pro.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_c");
        // ConsumerRebalanceListener 是分区监听器
        // 例如 topic_c 是5个分区，在起1个消费者的时候，全部分区都为改消费者，起2个以上的消费者开始重新分区，平衡每个消费者都有分区可以消费
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            // 回收之前的分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("revoke " + partition.topic() + "-->" + partition.partition());
                }
            }

            // 分配分区
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("assign " + partition.topic() + "-->" + partition.partition());
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
        }
    }


    /**
     * 粘性分区策略
     * 1、分区的分配要尽可能的均匀，分配给消费者者的主题分区数最多相差一个；
     * 2、分区的分配尽可能的与上次分配的保持相同。
     * 当两者发生冲突时，第一个目标优先于第二个目标。
     * StickyAssignor尽量保存之前的分区分配方案，分区重分配变动更小
     * <p>
     * 弊端：在Consumer Group有订阅消息不一致的情况下，我们最好不要选用RoundRobin
     */
    @Test
    public void stickyAssignor() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_group3");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        pro.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_c");
        // ConsumerRebalanceListener 是分区监听器
        // 例如 topic_c 是5个分区，在起1个消费者的时候，全部分区都为改消费者，起2个以上的消费者开始重新分区，平衡每个消费者都有分区可以消费
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            // 回收之前的分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("revoke " + partition.topic() + "-->" + partition.partition());
                }
            }

            // 分配分区
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("assign " + partition.topic() + "-->" + partition.partition());
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
        }
    }

    /**
     * 上述三种分区分配策略均是基于eager协议,Kafka2.4.0开始引入CooperativeStickyAssignor策略
     * CooperativeStickyAssignor与之前的StickyAssignor虽然都是维持原来的分区分配方案，最大的区别是：
     * StickyAssignor仍然是基于eager协议，分区重分配时候，都需要consumers先放弃当前持有的分区，重新加入consumer group；
     * 而CooperativeStickyAssignor基于cooperative协议，该协议将原来的一次全局分区重平衡，改成多次小规模分区重平衡。渐进式的重平衡。
     */
    @Test
    public void cooperativeStickyAssignor() {
        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, "btbox_group3");
        // pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        // pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeserializer.class.getName());
        pro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        pro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        pro.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(pro);
        List<String> topics = List.of("topic_c");
        // ConsumerRebalanceListener 是分区监听器
        // 例如 topic_c 是5个分区，在起1个消费者的时候，全部分区都为改消费者，起2个以上的消费者开始重新分区，平衡每个消费者都有分区可以消费
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            // 回收之前的分区
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("revoke " + partition.topic() + "-->" + partition.partition());
                }
            }

            // 分配分区
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("assign " + partition.topic() + "-->" + partition.partition());
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
        }
    }

}