package com.isolation.levels;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by dreambig on 11.03.17.
 */
public class ConnectionsProvider {


    private static final MysqlDataSource DATA_SOURCE = new MysqlDataSource();

    static {
        DATA_SOURCE.setPort(3306);
        DATA_SOURCE.setDatabaseName("sakila");
        DATA_SOURCE.setUser("sakila_user");
    }

    public static Connection getConnection() {
        try {
            return DATA_SOURCE.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to get connection", e);
        }
    }
}
