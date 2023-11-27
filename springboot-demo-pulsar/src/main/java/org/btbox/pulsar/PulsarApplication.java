package org.btbox.pulsar;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.pulsar.annotation.EnablePulsar;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/11/27 9:54
 * @version: 1.0
 */
@SpringBootApplication
@EnablePulsar
public class PulsarApplication {
    public static void main(String[] args) {
        SpringApplication.run(PulsarApplication.class, args);
    }
}