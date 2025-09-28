package org.example;

import java.util.List;

public class TransactionSet {
    private final int setNumber;
    // The list now holds the gRPC-defined Transaction objects
    private final List<MessageServiceOuterClass.Transaction> transactionList;
    private final List<String> activeNodesList;

    public TransactionSet(int setNumber) {
        this.setNumber = setNumber;
        this.transactionList = new java.util.ArrayList<>();
        this.activeNodesList = new java.util.ArrayList<>();
    }

    public TransactionSet(int setNumber, List<MessageServiceOuterClass.Transaction> transactionList, List<String> activeNodesList) {
        this.setNumber = setNumber;
        this.transactionList = transactionList;
        this.activeNodesList = activeNodesList;
    }

    // Getters
    public int getSetNumber() {
        return setNumber;
    }

    public List<MessageServiceOuterClass.Transaction> getTransactionList() {
        return transactionList;
    }

    public List<String> getActiveNodesList() {
        return activeNodesList;
    }

    public void addTransaction(MessageServiceOuterClass.Transaction transaction) {
        this.transactionList.add(transaction);
    }

    public void addActiveNodeList(List<String> nodes) {
        this.activeNodesList.addAll(nodes);
    }

    @Override
    public String toString() {
        return "Set " + setNumber +
                " | Transactions: " + transactionList.size() +
                " | Nodes: " + activeNodesList;
    }
}