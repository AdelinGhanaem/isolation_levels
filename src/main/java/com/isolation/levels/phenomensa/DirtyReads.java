package com.isolation.levels.phenomensa;

import com.isolation.levels.ConnectionsProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import static com.isolation.levels.Utils.printResultSet;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;

/**
 * Created by dreambig on 11.03.17.
 */
public class DirtyReads {
    public static void main(String[] args) {

        CountDownLatch t1 = new CountDownLatch(1);
        CountDownLatch t2 = new CountDownLatch(1);


        setUp(ConnectionsProvider.getConnection());


        Connection connection1 = ConnectionsProvider.getConnection();
        Connection connection2 = ConnectionsProvider.getConnection();

        assert connection1 != connection2;

        Transaction1 transaction1 = new Transaction1(t1, t2, connection1);
        Transaction2 transaction2 = new Transaction2(t1, t2, connection2);

        Thread thread1 = new Thread(transaction1);
        thread1.start();

        Thread thread2 = new Thread(transaction2);
        thread2.start();


    }
    private static void setUp(Connection connection) {

        try {
            connection.prepareStatement("delete from actor where first_name=\"Ivan\"").execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static class Transaction1 implements Runnable {

        private CountDownLatch t1Latch;
        private CountDownLatch t2Latch;

        private Connection connection;


        public Transaction1(CountDownLatch t1Latch, CountDownLatch t2Latch, Connection connection) {
            this.t1Latch = t1Latch;
            this.t2Latch = t2Latch;
            this.connection = connection;
        }

        public void run() {
            try {
                //start a transaction by setting auto commit to false
                connection.setAutoCommit(false);
                //Set the isolation level to of the transaction to TRANSACTION_READ_UNCOMMITTED
                connection.setTransactionIsolation(TRANSACTION_READ_UNCOMMITTED);

                //reading the first result
                ResultSet resultSet = connection.prepareStatement("select * from actor where first_name=\"Ivan\"").executeQuery();

                if (!resultSet.next())
                    System.out.println("There are not result when reads the query first time ! ");
                connection.commit();

                //wait here until thread 2 start a transaction and insert some rows.
                t2Latch.await();

                //now execute the same query
                ResultSet resultSet2 = connection.prepareStatement("select * from actor where first_name=\"Ivan\"").executeQuery();

                //now we have some results, even the the second transaction is still not committed !!
                printResultSet(resultSet2);
                connection.commit();
                // count down so that the transaction 2 can commit
                t1Latch.countDown();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static class Transaction2 implements Runnable {

        private CountDownLatch t1Latch;
        private CountDownLatch t2Latch;
        private Connection connection;

        public Transaction2(CountDownLatch t1Latch, CountDownLatch t2Latch, Connection connection) {
            this.t1Latch = t1Latch;
            this.t2Latch = t2Latch;
            this.connection = connection;
        }

        public void run() {
            try {
                connection.setAutoCommit(false);
                connection.prepareStatement("insert into actor (first_name,last_name,last_update) VALUE (\"Ivan\",\"Ivanov\",\"2019-02-01\")").execute();

                //let transaction 1 continue and isuue a select statement,
                // even this current transaction is not committed, transaction 1 will be able to see the inserted value but the statement above !
                t2Latch.countDown();

                //wait before commit, we want to make sure that this transaction is not committed and yet transaction 1 can still read the inserted row !
                t1Latch.await();
                // commit and exit !
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }
}
