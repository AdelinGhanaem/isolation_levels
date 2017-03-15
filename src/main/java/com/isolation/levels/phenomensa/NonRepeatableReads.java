package com.isolation.levels.phenomensa;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import static com.isolation.levels.ConnectionsProvider.getConnection;
import static com.isolation.levels.Utils.printResultSet;

/**
 * Created by dreambig on 12.03.17.
 */
public class NonRepeatableReads {


    public static void main(String[] args) {


        setUp(getConnection());
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread t1 = new Thread(new Transaction1(countDownLatch, getConnection()));
        Thread t2 = new Thread(new Transaction2(countDownLatch, getConnection()));

        t1.start();
        t2.start();

    }


    private static void setUp(Connection connection) {

        try {
            connection.prepareStatement("UPDATE actor SET last_name=\"WAYNE\" WHERE first_name=\"ALEC\"").execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    static class Transaction1 implements Runnable {


        private CountDownLatch t1Latch;
        private Connection connection;

        public Transaction1(CountDownLatch t1Latch, Connection connection) {
            this.t1Latch = t1Latch;
            this.connection = connection;
        }

        @Override
        public void run() {

            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                connection.setAutoCommit(false);
                // '29', 'ALEC', 'WAYNE', '2006-02-15 04:34:33'
                ResultSet firstResult = connection.prepareStatement("select * from actor where first_name=\"ALEC\"").executeQuery();
                printResultSet(firstResult);

                t1Latch.await();
                ResultSet secondResult = connection.prepareStatement("select * from actor where first_name=\"ALEC\"").executeQuery();
                printResultSet(secondResult);

                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }


    static class Transaction2 implements Runnable {


        private CountDownLatch t1Latch;
        private Connection connection;

        Transaction2(CountDownLatch t1Latch, Connection connection) {
            this.t1Latch = t1Latch;
            this.connection = connection;
        }


        @Override
        public void run() {

            try {
                connection.prepareStatement("UPDATE actor SET last_name=\"LAST_NAME_CHANGED\" WHERE first_name=\"ALEC\"").execute();
                t1Latch.countDown();
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }


    }
}
