// package org.btbox.kafka.config;
//
// import lombok.RequiredArgsConstructor;
// import org.apache.kafka.clients.producer.ProducerConfig;
// import org.apache.kafka.common.serialization.StringDeserializer;
// import org.apache.kafka.common.serialization.StringSerializer;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
// import org.springframework.context.annotation.Bean;
// import org.springframework.context.annotation.Configuration;
// import org.springframework.kafka.core.DefaultKafkaProducerFactory;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.kafka.core.ProducerFactory;
// import org.springframework.kafka.support.serializer.JsonSerializer;
// import org.springframework.kafka.transaction.KafkaTransactionManager;
//
// import java.util.HashMap;
// import java.util.Map;
//
// /**
//  * kafka配置，也可以写在yml，这个文件会覆盖yml
//  */
// @Configuration
// @RequiredArgsConstructor
// public class KafkaProviderConfig {
//
//     private final KafkaProperties kafkaProperties;
//
//     @Value("${spring.kafka.bootstrap-servers}")
//     private String bootstrapServers;
//     @Value("${spring.kafka.producer.transaction-id-prefix}")
//     private String transactionIdPrefix;
//     @Value("${spring.kafka.producer.acks}")
//     private String acks;
//     @Value("${spring.kafka.producer.retries}")
//     private String retries;
//     @Value("${spring.kafka.producer.batch-size}")
//     private String batchSize;
//     @Value("${spring.kafka.producer.buffer-memory}")
//     private String bufferMemory;
//
//     @Bean
//     public Map<String, Object> producerConfigs() {
//         Map<String, Object> props = new HashMap<>(16);
//         props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//         // acks=0 ： 生产者在成功写入消息之前不会等待任何来自服务器的响应。
//         // acks=1 ： 只要集群的首领节点收到消息，生产者就会收到一个来自服务器成功响应。
//         // acks=all ：只有当所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功响应。
//         // 开启事务必须设为all
//         props.put(ProducerConfig.ACKS_CONFIG, acks);
//         // 发生错误后，消息重发的次数，开启事务必须大于0
//         props.put(ProducerConfig.RETRIES_CONFIG, retries);
//         // 当多个消息发送到相同分区时,生产者会将消息打包到一起,以减少请求交互. 而不是一条条发送
//         // 批次的大小可以通过batch.size 参数设置.默认是16KB
//         // 较小的批次大小有可能降低吞吐量（批次大小为0则完全禁用批处理）。
//         // 比如说，kafka里的消息5秒钟Batch才凑满了16KB，才能发送出去。那这些消息的延迟就是5秒钟
//         // 实测batchSize这个参数没有用
//         props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
//
//         // 生产者内存缓冲区的大小
//         props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
//         // 反序列化，和生产者的序列化方式对应
//         props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//         props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//
//         props.putAll(kafkaProperties.getProducer().getProperties());
//         // ProducerConfig
//         return props;
//     }
//
//     @Bean
//     public ProducerFactory<String, Object> producerFactory() {
//         DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(producerConfigs());
//         // 开启事务，会导致 LINGER_MS_CONFIG 配置失效
//         factory.setTransactionIdPrefix(transactionIdPrefix);
//         return factory;
//     }
//
//     @Bean
//     public KafkaTransactionManager<String, Object> kafkaTransactionManager(ProducerFactory<String, Object> producerFactory) {
//         return new KafkaTransactionManager<>(producerFactory);
//     }
//
//     @Bean
//     public KafkaTemplate<String, Object> kafkaTemplate() {
//         return new KafkaTemplate<>(producerFactory());
//     }
// }