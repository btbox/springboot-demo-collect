package org.btbox.kafka.domain.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/7 16:30
 * @version: 1.0
 */
@Data
public class Person implements Serializable {

    private String username;

    private int age;

}