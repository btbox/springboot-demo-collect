package org.btbox.kafka.client.consume;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.btbox.kafka.client.Student;
import org.btbox.kafka.client.common.CommonConstant;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/5 15:42
 * @version: 1.0
 */
public class CustomConsumerOffsetWithStorageMysql {

    /**
     * offset偏移量存储到redis
     */
    @Test
    public void offsetStorageMysql() throws SQLException {
        // 声明redis客户端
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "root");
        PreparedStatement selectPrp = connection.prepareStatement("select * from kafka.my_offset where group_id = ? and topic = ? and `partition` = ?");
        PreparedStatement insertPrp = connection.prepareStatement("replace into kafka.my_offset(group_id, topic, `partition`, offset) values (?,?,?,?)");


        String groupId = "btbox_offset_storage_mysql";

        Properties pro = new Properties();
        pro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstant.CLUSTER_SERVICE_URL);
        pro.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
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
                for (TopicPartition tp : partitions) {
                    String topic = tp.topic();
                    int partition = tp.partition();
                    try {
                        selectPrp.setString(1, groupId);
                        selectPrp.setString(2, topic);
                        selectPrp.setInt(3, partition);
                        ResultSet result = selectPrp.executeQuery();
                        while (result.next()) {
                            long offset = result.getLong("offset");
                            consumer.seek(tp, offset + 1);
                        }
                        selectPrp.clearParameters();
                    } catch (Exception e) {
                        e.printStackTrace();
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

                // 存放到mysql
                insertPrp.setString(1, groupId);
                insertPrp.setString(2, record.topic());
                insertPrp.setInt(3, record.partition());
                insertPrp.setLong(4, record.offset());
                insertPrp.execute();
                insertPrp.clearParameters();

            }
            // 手动同步提交,提交失败则一直重试
            // consumer.commitSync();
            // 手动异步提交,只提交一次，不管是否失败，但是还是会记录最后一次提交成功的offset一直递增所以不怕失败，至少成功最后一次即可，大部分情况用这个
            consumer.commitAsync();
        }
    }
}