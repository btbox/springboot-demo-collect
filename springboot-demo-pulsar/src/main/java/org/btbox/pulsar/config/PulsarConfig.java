package org.btbox.pulsar.config;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;

import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/27 17:54
 * @version: 1.0
 */
@Configuration
public class PulsarConfig {

    // @Bean
    // public ConsumerBuilderCustomizer<String> myCustomizer() {
    //     return (builder) -> builder
    //             .consumerName("myConsumer")
    //             .subscriptionName("sub_01")
    //             .batchReceivePolicy(BatchReceivePolicy.builder()
    //                     // 最大消息大小
    //                     .maxNumBytes(1024 * 1024)
    //                     // 每次接受消息最大数量
    //                     .maxNumMessages(3)
    //                     // 隔多长时间获取一次
    //                     .timeout(5, TimeUnit.SECONDS)
    //                     .build()
    //             )
    //             ;
    // }

}