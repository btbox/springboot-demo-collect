package org.btbox.rocketmq.clientapi.demo;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.MessageQueue;
import org.btbox.rocketmq.clientapi.domain.MsgModel;
import org.btbox.rocketmq.constants.MqConstant;
import org.junit.jupiter.api.Test;
import org.apache.rocketmq.common.message.Message;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @description: 顺序消费
 * @author: BT-BOX
 * @createDate: 2025/7/9 18:20
 * @version: 1.0
 */
public class OrderlyTest {

    private List<MsgModel> msgModels = Arrays.asList(
            new MsgModel("qwer", 1, "下单"),
            new MsgModel("qwer", 1, "短信"),
            new MsgModel("qwer", 1, "物流")
            );

    @Test
    public void orderlyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ms-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();


        msgModels.forEach(msgModel -> {
            Message message = new Message("orderylyTopic", msgModel.toString().getBytes());
            try {

                producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        return null;
                    }
                }, msgModel.getOrderSn());

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        producer.send(message);
        producer.shutdown();
    }

}