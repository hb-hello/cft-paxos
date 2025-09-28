package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        String filePath = "src/main/resources/transactionSets.csv";

        Logger logger = LogManager.getLogger(Main.class);

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