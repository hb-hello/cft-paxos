package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        final String FILE_PATH = "src/main/resources/transactionSets.csv";
        final long TIMEOUT_MILLIS = 500;
        final int MAX_RETRIES = 5;

        Logger logger = LogManager.getLogger(Main.class);

//        TODO: Start all server processes

        HashMap<Integer, TransactionSet> transactionSets;

        try {
            transactionSets = TransactionSetLoader.loadTransactionSets(FILE_PATH);
            for (int setNumber : transactionSets.keySet()) {

                TransactionSet transactionSet = transactionSets.get(setNumber);

                try (ExecutorService executor = Executors.newFixedThreadPool(transactionSet.transactions().size())) {
//                  Loop through all senders in the transaction set
//                  Key in the transactions map represents the sender
                    for (String clientId : transactionSet.transactions().keySet()) {
                        executor.submit(() -> {
                            try {
                                Client client = new Client(clientId);
                                logger.info("Client {} initialized.", clientId);
                                for (MessageServiceOuterClass.Transaction transaction : transactionSet.transactions().get(clientId)) {
                                    client.processTransaction(transaction, TIMEOUT_MILLIS, MAX_RETRIES);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                }
            }
        } catch (java.lang.Exception e) {
            logger.error("Error loading transaction sets: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}