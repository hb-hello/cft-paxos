package org.example;

import java.util.*;

/**
 * @param setNumber       Transaction set identifier
 * @param transactionEvents    List of events which could be Transaction or LeaderFailure
 * @param activeNodesList List of active nodes for the transaction set
 */
public record TransactionSet(int setNumber, List<TransactionEvent> transactionEvents,
                             List<String> activeNodesList) {
    public TransactionSet(int setNumber) {
        this(setNumber, new ArrayList<>(), new ArrayList<>());
    }

    public void addTransactionEvent(TransactionEvent event) {
        this.transactionEvents.add(event);
    }

    public void addActiveNodesList(List<String> nodes) {
        this.activeNodesList.addAll(nodes);
    }

    /**
     * Groups transactions by sender ID, with each phase separated by LeaderFailure events.
     *
     * Returns a list where each element represents a phase:
     * - Phase 0: transactions before the first LeaderFailure
     * - Phase 1: transactions between first and second LeaderFailure
     * - Phase N: transactions after the Nth LeaderFailure
     *
     * Within each phase, transactions are grouped by sender ID.
     *
     * Example for Set 9:
     *   Events: [Transaction(C->H:3), LeaderFailure(), Transaction(E->D:1),
     *            Transaction(G->I:2), LeaderFailure(), Transaction(A->J:1)]
     *
     *   Result: [
     *     Phase 0: {C: [Transaction(C->H:3)]},
     *     Phase 1: {E: [Transaction(E->D:1)], G: [Transaction(G->I:2)]},
     *     Phase 2: {A: [Transaction(A->J:1)]}
     *   ]
     *
     * @return List of phases, where each phase is a HashMap of sender -> List of Transactions
     */
    public List<Map<String, List<Transaction>>> groupTransactionsBySenderPerPhase() {
        List<Map<String, List<Transaction>>> phases = new ArrayList<>();
        Map<String, List<Transaction>> currentPhase = new HashMap<>();

        for (TransactionEvent event : transactionEvents) {
            if (event instanceof LeaderFailure) {
                // End current phase and start a new one
                if (!currentPhase.isEmpty()) {
                    phases.add(currentPhase);
                    currentPhase = new HashMap<>();
                }
                // Note: We don't add the LeaderFailure itself to the phase structure
                // It acts as a delimiter between phases
            } else if (event instanceof Transaction) {
                Transaction tx = (Transaction) event;
                String sender = tx.getSender();

                // Add transaction to current phase, grouped by sender
                currentPhase.computeIfAbsent(sender, k -> new ArrayList<>()).add(tx);
            }
        }

        // Don't forget the last phase (after the last LeaderFailure or if no LeaderFailures exist)
        if (!currentPhase.isEmpty()) {
            phases.add(currentPhase);
        }

        return phases;
    }

    /**
     * Utility method to print the phase structure for debugging
     */
    public void printPhases() {
        List<Map<String, List<Transaction>>> phases = groupTransactionsBySenderPerPhase();

        System.out.println("TransactionSet #" + setNumber + " [Nodes: " + activeNodesList + "]");
        System.out.println("Total Phases: " + phases.size());

        for (int phaseIndex = 0; phaseIndex < phases.size(); phaseIndex++) {
            Map<String, List<Transaction>> phase = phases.get(phaseIndex);
            System.out.println("\n  Phase " + phaseIndex + ":");

            for (Map.Entry<String, List<Transaction>> entry : phase.entrySet()) {
                String sender = entry.getKey();
                List<Transaction> transactions = entry.getValue();
                System.out.println("    Sender '" + sender + "': " + transactions.size() + " transaction(s)");

                for (Transaction tx : transactions) {
                    System.out.println("      - " + tx);
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("TransactionSet #%d [Nodes: %s]\n", setNumber, activeNodesList));
        for (int i = 0; i < transactionEvents.size(); i++) {
            sb.append(String.format("  %d. %s\n", i + 1, transactionEvents.get(i)));
        }
        return sb.toString();
    }
}