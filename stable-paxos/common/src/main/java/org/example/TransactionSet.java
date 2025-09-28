package org.example;

import java.util.ArrayList;
import java.util.List;

/**
 * @param setNumber Transaction set identifier
 * @param transactionList List of gRPC-defined Transaction objects in the transaction set
 * @param activeNodesList List of active nodes for the transaction set
 */
public record TransactionSet(int setNumber, List<MessageServiceOuterClass.Transaction> transactionList,
                             List<String> activeNodesList) {
    public TransactionSet(int setNumber) {
        this(setNumber, new ArrayList<>(), new ArrayList<>());
    }

    public void addTransaction(MessageServiceOuterClass.Transaction transaction) {
        this.transactionList.add(transaction);
    }

    public void addActiveNodeList(List<String> nodes) {
        this.activeNodesList.addAll(nodes);
    }

    @Override
    public String toString() {
        return "Set " + setNumber + " | Transactions: " + transactionList.size() + " | Nodes: " + activeNodesList;
    }
}