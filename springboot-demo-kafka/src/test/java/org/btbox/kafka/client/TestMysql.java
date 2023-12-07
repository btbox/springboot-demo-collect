package org.btbox.kafka.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2023/12/6 15:53
 * @version: 1.0
 */
public class TestMysql {

    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "root");
        PreparedStatement ps = connection.prepareStatement("create database kafka");
        ps.execute();

    }

}