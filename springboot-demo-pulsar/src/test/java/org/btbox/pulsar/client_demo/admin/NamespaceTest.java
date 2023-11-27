package org.btbox.pulsar.client_demo.admin;

import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_8080;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/24 14:47
 * @version: 1.0
 */
public class NamespaceTest {


    @Test
    @SneakyThrows
    public void createNameSpace() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(SERVICE_HTTP_URL_8080).build();

        pulsarAdmin.namespaces().createNamespace("btbox_pulsar_t/btbox_pulsar_n");

        pulsarAdmin.close();
    }

    @Test
    @SneakyThrows
    public void listNameSpace() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(SERVICE_HTTP_URL_8080).build();

        List<String> namespaces = pulsarAdmin.namespaces().getNamespaces("btbox_pulsar_t");
        for (String namespace : namespaces) {
            System.out.println("namespace = " + namespace);
        }

        pulsarAdmin.close();
    }

    @Test
    @SneakyThrows
    public void deleteNameSpace() {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(SERVICE_HTTP_URL_8080).build();

        pulsarAdmin.namespaces().deleteNamespace("btbox_pulsar_t/btbox_pulsar_n");

        pulsarAdmin.close();
    }
}