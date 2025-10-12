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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionSetLoader {

    // Regex to extract (Item1, Item2, Value) from the Transaction string
    private static final Pattern TRANSACTION_PATTERN = Pattern.compile("\\(([A-Z]+),\\s*([A-Z]+),\\s*(\\d+)\\)");
    private static final String LEADER_FAILURE_MARKER = "LF";

    /**
     * Load transaction sets from CSV file using openCSV
     * CSV format: Set Number, Transactions, Live Nodes
     * - Each set contains a sequence of Transaction and LeaderFailure events
     * - Events are stored in the order they appear in the CSV
     */
    public static HashMap<Integer, TransactionSet> loadTransactionSets(String filePath) {

        HashMap<Integer, TransactionSet> transactionSets = new HashMap<>();

        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath)).withSkipLines(1).build()) {
            String[] nextLine;
            Integer currentSetNumber = null;

            while ((nextLine = reader.readNext()) != null) {
                // Column 0: Set Number (only present on first row of each set)
                if (nextLine[0] != null && !nextLine[0].trim().isEmpty()) {
                    currentSetNumber = Integer.parseInt(nextLine[0].trim());
                }

                if (currentSetNumber == null) {
                    throw new IllegalStateException("Encountered row without a set number being established");
                }


                // Get or create the TransactionSet
                TransactionSet transactionSet;
                if (transactionSets.containsKey(currentSetNumber)) {
                    transactionSet = transactionSets.get(currentSetNumber);
                } else {
                    transactionSet = new TransactionSet(currentSetNumber);
                    transactionSets.put(currentSetNumber, transactionSet);
                }

                // Column 2: Live Nodes (only present on first row of each set)
                if (nextLine.length > 2 && nextLine[2] != null && !nextLine[2].trim().isEmpty()) {
                    transactionSet.addActiveNodesList(parseNodes(nextLine[2]));
                }

                // Column 1: Transactions or Leader Failure marker
                if (nextLine.length > 1 && nextLine[1] != null && !nextLine[1].trim().isEmpty()) {
                    String transactionStr = nextLine[1].trim();

                    // Check if this is a leader failure event
                    if (LEADER_FAILURE_MARKER.equals(transactionStr)) {
                        transactionSet.addTransactionEvent(new LeaderFailure());
                    } else {
                        // Parse and add actual transaction
                        Transaction transaction = parseTransaction(transactionStr);
                        transactionSet.addTransactionEvent(transaction);
                    }
                }

//                transactionSet.addTransactionEvent(parseTransaction(nextLine[1]));
            }

            return transactionSets;

        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + filePath, e);
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }

    }


    // Parses the transaction string into the gRPC-like Transaction object
    private static Transaction parseTransaction(String transactionStr) {
        Matcher matcher = TRANSACTION_PATTERN.matcher(transactionStr);
        if (matcher.find()) {
            String sender = matcher.group(1);
            String receiver = matcher.group(2);
            // The value is parsed as a double for the 'amount' field
            double amount = Double.parseDouble(matcher.group(3));

            return new Transaction(sender, receiver, amount);
        }
        throw new IllegalArgumentException("Invalid transaction format: " + transactionStr);
    }

    // Parses the comma-separated list of nodes
    private static List<String> parseNodes(String liveNodesStr) {
        // Remove square brackets if present
        String cleanedStr = liveNodesStr.replaceAll("[\\[\\]]", "");

        // Split by comma
        String[] nodesArray = cleanedStr.split(",");

        List<String> nodesList = new ArrayList<>();
        for (String node : nodesArray) {
            String trimmedNode = node.trim();
            // Only add non-empty nodes (handles edge cases like empty brackets)
            if (!trimmedNode.isEmpty()) {
                nodesList.add(trimmedNode);
            }
        }

        return nodesList;
    }

}
