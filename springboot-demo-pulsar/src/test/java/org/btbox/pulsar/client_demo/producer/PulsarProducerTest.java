package org.btbox.pulsar.client_demo.producer;

import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_6650;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/24 15:38
 * @version: 1.0
 */
public class PulsarProducerTest {

    @Test
    @SneakyThrows
    public void sendSync() {

        // 高版本jdk中出现以下错误 Pulsar的DnsResolverUtil需要深度反射sun.net.InetAddressCachePolicy (in module java.base)的时候会没有权限，导致报错
        // [main] WARN org.apache.pulsar.common.util.netty.DnsResolverUtil -- Cannot get DNS TTL settings from sun.net.InetAddressCachePolicy class
        // java.lang.IllegalAccessException: class org.apache.pulsar.common.util.netty.DnsResolverUtil cannot access class sun.net.InetAddressCachePolicy (in module java.base) because module java.base does not export sun.net to unnamed module @3e6fa38a
        // 问题: https://github.com/apache/pulsar/issues/20282
        // 相关博客: https://blog.csdn.net/AsterCass/article/details/134447886
        // 添加如下参数在启用java的时候
        // --add-opens
        // java.base/java.lang=ALL-UNNAMED
        //         --add-opens
        // java.base/java.util=ALL-UNNAMED
        //         --add-opens
        // java.base/sun.net=ALL-UNNAMED

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(SERVICE_HTTP_URL_6650).build();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl")
                .create();
        for (int i = 0; i < 20; i++) {
            producer.send("hello java API pulsar ..." + i);
        }
        producer.close();
        pulsarClient.close();
    }

    /**
     * 异步发送
     */
    @Test
    @SneakyThrows
    public void sendASync() {

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(SERVICE_HTTP_URL_6650).build();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://btbox_pulsar_t/btbox_pulsar_n/t_topicl")
                .create();
        // 异步发送没有马上生成成功，因为是异步发送，如果没有等待，则主线程的已经执行了producer.close() pulsarClient.close() 关闭资源，所以要现场等待
        producer.sendAsync("hello async API pulsar ...");

        // 主线程睡眠1秒
        TimeUnit.SECONDS.sleep(1);
        producer.close();
        pulsarClient.close();
    }


}