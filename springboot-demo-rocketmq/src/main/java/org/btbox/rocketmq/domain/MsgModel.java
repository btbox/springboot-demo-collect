package org.btbox.rocketmq.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2025/7/9 18:20
 * @version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MsgModel {

    private String orderSn;

    private Integer userId;

    private String desc;

}