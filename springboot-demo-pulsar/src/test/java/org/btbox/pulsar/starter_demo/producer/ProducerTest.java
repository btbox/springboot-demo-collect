package org.btbox.pulsar.starter_demo.producer;

import lombok.SneakyThrows;
import org.btbox.pulsar.pojo.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTemplate;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/27 14:52
 * @version: 1.0
 */
@SpringBootTest
public class ProducerTest {

    @Autowired
    private PulsarTemplate pulsarTemplate;

    @Test
    @SneakyThrows
    public void send() {

        // for (int i = 0; i < 50; i++) {
            User user = new User();
            user.setName("btbox");
            user.setAge(20);
            user.setAddress("北京");
            pulsarTemplate.send("persistent://btbox_pulsar_t/btbox_pulsar_n/t_user1", user);

        // }
    }

}