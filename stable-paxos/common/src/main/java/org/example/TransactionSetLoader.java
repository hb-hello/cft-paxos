package org.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionSetLoader {

    // Regex to extract (Item1, Item2, Value) from the Transaction string
    private static final Pattern TRANSACTION_PATTERN = Pattern.compile("\\(([A-Z]+),\\s*([A-Z]+),\\s*(\\d+)\\)");

//    Load transaction sets from src/main/resources/transactionSets.csv using openCSV

    public static Map<Integer, TransactionSet> loadTransactionSets(String filePath) {

        Map<Integer, TransactionSet> transactionSets = new HashMap<>();

        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath)).withSkipLines(1).build()) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                int setNumber = Integer.parseInt(nextLine[0]);
                TransactionSet transactionSet;

                if (transactionSets.containsKey(setNumber)) transactionSet = transactionSets.get(setNumber);
                else {
                    transactionSet = new TransactionSet(setNumber);
                    transactionSets.put(setNumber, transactionSet);
                }


                transactionSet.addTransaction(parseTransaction(nextLine[1]));
                transactionSet.addActiveNodeList(parseNodes(nextLine[2]));
            }

            return transactionSets;

        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + filePath, e);
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }

    }


    // Parses the transaction string into the gRPC-like Transaction object
    private static MessageServiceOuterClass.Transaction parseTransaction(String transactionStr) {
        Matcher matcher = TRANSACTION_PATTERN.matcher(transactionStr);
        if (matcher.find()) {
            String sender = matcher.group(1);
            String receiver = matcher.group(2);
            // The value is parsed as a double for the 'amount' field
            double amount = Double.parseDouble(matcher.group(3));

            return MessageServiceOuterClass.Transaction.newBuilder().setSender(sender).setReceiver(receiver).setAmount(amount).build();
        }
        throw new IllegalArgumentException("Invalid transaction format: " + transactionStr);
    }

    // Parses the comma-separated list of nodes
    private static List<String> parseNodes(String liveNodesStr) {
        String[] nodesArray = liveNodesStr.split(",");
        List<String> nodesList = new ArrayList<>();
        for (String node : nodesArray) {
            nodesList.add(node.trim());
        }
        return nodesList;
    }

}
