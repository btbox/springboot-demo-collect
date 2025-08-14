package org.btbox.rocketmq.clientapi.demo;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.message.Message;
import org.btbox.rocketmq.constants.MqConstant;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2025/7/25 10:54
 * @version: 1.0
 */
public class TransactionTest {


    @Test
    public void producer() throws MQClientException, InterruptedException {

        //1.定义事务监听器
        TransactionListener transactionListener = new TransactionListenerImpl();
        //2.定义生产者
        TransactionMQProducer producer = new TransactionMQProducer("transaction-produce-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);


        //3.定义线程池
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 5, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100), (runnable, executor) -> {
            BlockingQueue<Runnable> queue = executor.getQueue();
            try {
                queue.put(runnable);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        //4.设置线程池
        producer.setExecutorService(threadPoolExecutor);
        //5.设置事务监听器
        producer.setTransactionListener(transactionListener);
        // 启动生产者
        producer.start();
        String[] tags = {"tagA", "tagB", "tagC", "tagD","tagE"};
        //发送10条half消息，消费者是收不到half消息的
        for (int i = 0; i < 10; i++) {
            Message message = new Message("TransactionTopic", tags[i % tags.length],
                    "key" + i, ("飞哥测试事务消息" + tags[i % tags.length]+"_"+i).getBytes(StandardCharsets.UTF_8));
            TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, null);
            System.out.println("本次发送的消息是=" + new String(message.getBody()));
            System.out.printf("%s%n", transactionSendResult);
            Thread.sleep(10);
        }
        System.out.println("==========所有消息发送完成======");
    }

}