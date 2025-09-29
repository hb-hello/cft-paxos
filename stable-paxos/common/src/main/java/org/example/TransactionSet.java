package org.example;

import java.util.*;

/**
 * @param setNumber       Transaction set identifier
 * @param transactions    List of gRPC-defined Transaction objects in the transaction set
 * @param activeNodesList List of active nodes for the transaction set
 */
public record TransactionSet(int setNumber, Map<String, List<MessageServiceOuterClass.Transaction>> transactions,
                             List<String> activeNodesList) {
    public TransactionSet(int setNumber) {
        this(setNumber, new HashMap<>(), new ArrayList<>());
    }

    public void addTransaction(MessageServiceOuterClass.Transaction transaction) {
        String sender = transaction.getSender();
        if (!transactions.containsKey(sender)) {
            transactions.put(sender, new ArrayList<>());
        }
        transactions.get(sender).add(transaction);
    }

    public void addActiveNodeList(List<String> nodes) {
        this.activeNodesList.addAll(nodes);
    }

    @Override
    public String toString() {
        return "Set " + setNumber + " | Transactions: " + transactions.size() + " | Nodes: " + activeNodesList;
    }
}