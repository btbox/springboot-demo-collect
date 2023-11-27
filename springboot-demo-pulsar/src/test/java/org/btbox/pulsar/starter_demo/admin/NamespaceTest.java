package org.btbox.pulsar.starter_demo.admin;

import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.pulsar.core.PulsarAdministration;

import java.util.List;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_8080;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/24 14:47
 * @version: 1.0
 */
@SpringBootTest
public class NamespaceTest {

    @Autowired
    private PulsarAdministration pulsarAdministration;

    @Test
    @SneakyThrows
    public void createNameSpace() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();

        pulsarAdmin.namespaces().createNamespace("btbox_pulsar_t/btbox_pulsar_n");

        pulsarAdmin.close();
    }

    @Test
    @SneakyThrows
    public void listNameSpace() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();

        List<String> namespaces = pulsarAdmin.namespaces().getNamespaces("btbox_pulsar_t");
        for (String namespace : namespaces) {
            System.out.println("namespace = " + namespace);
        }

        pulsarAdmin.close();
    }

    @Test
    @SneakyThrows
    public void deleteNameSpace() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();

        pulsarAdmin.namespaces().deleteNamespace("btbox_pulsar_t/btbox_pulsar_n");

        pulsarAdmin.close();
    }
}