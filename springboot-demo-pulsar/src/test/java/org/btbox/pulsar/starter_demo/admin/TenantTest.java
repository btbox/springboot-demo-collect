package org.btbox.pulsar.starter_demo.admin;

import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.pulsar.core.PulsarAdministration;

import java.util.List;
import java.util.Set;

import static org.btbox.pulsar.common.PulsarCommon.SERVICE_HTTP_URL_8080;
import static org.btbox.pulsar.common.PulsarCommon.STANDALONE;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/24 11:57
 * @version: 1.0
 */
@SpringBootTest
public class TenantTest {

    @Autowired
    private PulsarAdministration pulsarAdministration;

    /**
     * 创建租户
     */
    @SneakyThrows
    @Test
    public void createTenant() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();

        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Set.of(STANDALONE))
                .build();
        pulsarAdmin.tenants().createTenant("btbox_pulsar_t", tenantInfo);

        pulsarAdmin.close();
    }


    /**
     * 租户列表
     */
    @SneakyThrows
    @Test
    public void listTenant() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();

        List<String> tenants = pulsarAdmin.tenants().getTenants();
        for (String tenant : tenants) {
            System.out.println("租户信息: " + tenant);
        }

        pulsarAdmin.close();
    }

    /**
     * 查看某个租户信息
     */
    @SneakyThrows
    @Test
    public void tenantInfo() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();

        TenantInfo tenant = pulsarAdmin.tenants().getTenantInfo("btbox_pulsar_t");
        System.out.println("tenant = " + tenant);

        pulsarAdmin.close();
    }

    /**
     * 删除某个租户
     */
    @SneakyThrows
    @Test
    public void deleteTenant() {
        PulsarAdmin pulsarAdmin = pulsarAdministration.createAdminClient();

        pulsarAdmin.tenants().deleteTenant("btbox_pulsar_t");
        // 是否强制删除租户 force
        // pulsarAdmin.tenants().deleteTenant("btbox_pulsar_t", true);
        // 异步删除租户
        // pulsarAdmin.tenants().deleteTenantAsync("btbox_pulsar_t");

        pulsarAdmin.close();
    }
}