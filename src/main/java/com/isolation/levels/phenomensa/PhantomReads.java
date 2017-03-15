package com.isolation.levels.phenomensa;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import static com.isolation.levels.ConnectionsProvider.getConnection;
import static com.isolation.levels.Utils.printResultSet;

/**
 * Created by dreambig on 13.03.17.
 */
public class PhantomReads {


    public static void main(String[] args) {


        setUp(getConnection());// delete the newly inserted row, the is supposed to be a phantom row
        CountDownLatch countDownLatch1 = new CountDownLatch(1);  // use to synchronize threads steps
        CountDownLatch countDownLatch2 = new CountDownLatch(1);  // use to synchronize threads steps

        Transaction1 transaction1 = new Transaction1(countDownLatch1, countDownLatch2, getConnection()); // the first runnable
        Transaction2 transaction2 = new Transaction2(countDownLatch1, countDownLatch2, getConnection()); // the second runnable

        Thread thread1 = new Thread(transaction1); // transaction 1
        Thread thread2 = new Thread(transaction2); // transaction 2

        thread1.start();
        thread2.start();

    }

    private static void setUp(Connection connection) {

        try {
            connection.prepareStatement("DELETE from actor where last_name=\"PHANTOM_READ\"").execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static class Transaction1 implements Runnable {


        private CountDownLatch countDownLatch;
        private CountDownLatch countDownLatch2;
        private Connection connection;


        public Transaction1(CountDownLatch countDownLatch, CountDownLatch countDownLatch2, Connection connection) {
            this.countDownLatch = countDownLatch;
            this.countDownLatch2 = countDownLatch2;
            this.connection = connection;
        }

        @Override
        public void run() {


            try {

                String query = "select * from actor where first_name=\"BELA\"";

                connection.setAutoCommit(false); // start the transaction

                // the transaction isolation, dirty reads and non-repeatable reads are prevented !
                // only phantom reads can occure
                connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

                //read the query result for the first time.
                ResultSet resultSet = connection.prepareStatement(query).executeQuery();
                printResultSet(resultSet);                 // print result.


                //count down so that thread2 can insert a row and commit.
                countDownLatch2.countDown();
                //wait for the second query the finish inserting the row
                countDownLatch.await();

                System.out.println("\n ********* The query returns a second row satisfies it (a phantom read) ********* !");
                // Because MySQL has its own behaviour dealing with phantom reads we need to  execute an update in order to demonstrate the phantom read.
                //For more information check this answer http://stackoverflow.com/questions/42794425/unable-to-produce-a-phantom-read/42796969#42796969
                connection.prepareStatement("UPDATE actor set last_name=\"PHANTOM READ!!\" where last_name=\"PHANTOM_READ\"").execute();

                //query the result again ...
                ResultSet secondRead = connection.createStatement().executeQuery(query);

                printResultSet(secondRead);  //print the result

            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }


    public static class Transaction2 implements Runnable {


        private CountDownLatch countDownLatch;
        private CountDownLatch countDownLatch2;
        private Connection connection;


        public Transaction2(CountDownLatch countDownLatch, CountDownLatch countDownLatch2, Connection connection) {
            this.countDownLatch = countDownLatch;
            this.countDownLatch2 = countDownLatch2;
            this.connection = connection;
        }


        @Override
        public void run() {

            try {
                //wait the first thread to read the result
                countDownLatch2.await();

                //insert and commit !
                connection.prepareStatement("INSERT INTO actor (first_name,last_name) VALUE (\"BELA\",\"PHANTOM_READ\") ").execute();
                //count down so that the thread1 can read the result again ...
                countDownLatch.countDown();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }


}
