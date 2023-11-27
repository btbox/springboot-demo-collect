// package org.btbox.pulsar;
//
// import org.apache.pulsar.client.api.BatchReceivePolicy;
// import org.apache.pulsar.client.api.Consumer;
// import org.apache.pulsar.client.api.PulsarClient;
// import org.apache.pulsar.client.impl.schema.AvroSchema;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.context.annotation.Bean;
// import org.springframework.context.annotation.Configuration;
//
// import java.util.concurrent.TimeUnit;
//
// /**
//  * @description:
//  * @author: BT-BOX
//  * @createDate: 2023/11/27 17:54
//  * @version: 1.0
//  */
// @Configuration
// public class PulsarConfig {
//
//     @Bean
//     public Consumer<String> myCustomizer() {
//
//         Consumer<String> consumer = pulsarClient.newConsumer(AvroSchema.STRING)
//                 .topic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl")
//                 .subscriptionName("sub_01")
//                 .batchReceivePolicy(BatchReceivePolicy.builder()
//                         // 最大消息大小
//                         .maxNumBytes(1024 * 1024)
//                         // 每次接受消息最大数量
//                         .maxNumMessages(5)
//                         // 隔多长时间获取一次
//                         .timeout(5, TimeUnit.SECONDS)
//                         .build()
//                 )
//                 .subscribe();
//     }
//
// }