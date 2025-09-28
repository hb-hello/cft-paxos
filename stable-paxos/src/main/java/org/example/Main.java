package org.example;

public class Main {
    public static void main(String[] args) {

        String filePath = "src/main/resources/transactionSets.csv";

        Map<Integer, TransactionSet> transactionSets;

        try {
            transactionSets = TransactionSetLoader.loadTransactionSets(filePath);
            System.out.println(transactionSets);
        } catch (java.lang.Exception e) {
            logger.error("Error loading transaction sets: " + e.getMessage());
            throw new RuntimeException(e);
        }

    }
}