package com.isolation.levels;

import javax.sql.RowSet;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dreambig on 11.03.17.
 */
public class Utils {


    public static void printResultSet(ResultSet resultSet) throws SQLException {




        List<String> columnNames= new ArrayList<String>();

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            columnNames.add(resultSetMetaData.getColumnName(i));
        }
        while (resultSet.next()) {

            System.out.println("==========================================================");
            columnNames.forEach(c->{
                try {
                    Object o = resultSet.getObject(c);
                    System.out.print(" | ");
                    System.out.print(o);
                    System.out.print(" | ");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("\n==========================================================");
        }


    }
}
