package org.example;

import java.util.*;
import java.util.concurrent.*;

public class TransactionSetExecutor {

    private final TransactionSet transactionSet;
    private final ExecutorService executorService;

    // Callback interfaces for custom logic
    public interface TransactionProcessor {
        void process(String sender, List<Transaction> transactions) throws Exception;
    }

    public interface LeaderFailureHandler {
        void handle(int leaderFailureCount) throws Exception;
    }

    public TransactionSetExecutor(TransactionSet transactionSet, int maxThreads) {
        this.transactionSet = transactionSet;
//        this.executorService = Executors.newCachedThreadPool();
        this.executorService = Executors.newFixedThreadPool(150);
    }

    /**
     * Executes transaction events with the following behavior:
     *
     * 1. Process events sequentially in order
     * 2. When transactions are encountered, accumulate them by sender
     * 3. When LeaderFailure is encountered:
     *    a. Spawn threads for all accumulated transactions (async)
     *    b. BLOCK and handle leader failure
     *    c. Continue to next events
     * 4. At the end, spawn threads for any remaining accumulated transactions
     *
     * FIXED: Now properly handles all transactions including those after the last LF
     *
     * Example for Set 9:
     *   - Accumulate: C's transaction
     *   - Hit LF → Spawn thread for C (async), then BLOCK on LF handler
     *   - Accumulate: E and G transactions
     *   - Hit LF → Spawn threads for E and G (async), then BLOCK on LF handler
     *   - Accumulate: A's transaction
     *   - End of events → Spawn thread for A (async)
     */
    public void executeEvents(TransactionProcessor transactionProcessor,
                              LeaderFailureHandler leaderFailureHandler) throws InterruptedException, ExecutionException {

        List<TransactionEvent> events = transactionSet.transactionEvents();

        System.out.println("╔════════════════════════════════════════════════════════════╗");
        System.out.println("║ Starting execution of TransactionSet #" + transactionSet.setNumber());
        System.out.println("║ Total events in list: " + events.size());
        System.out.println("║ Active nodes: " + transactionSet.activeNodesList());
        System.out.println("╚════════════════════════════════════════════════════════════╝");

        // DEBUG: Print all events first
        System.out.println("\n=== DEBUG: All events in set ===");
        for (int i = 0; i < events.size(); i++) {
            System.out.println("  Event " + (i+1) + ": " + events.get(i));
        }
        System.out.println("=== End of events list ===\n");

        // Track transactions between leader failures (or from start to end)
        Map<String, List<Transaction>> currentPhaseSenders = new LinkedHashMap<>();
        int leaderFailureCount = 0;
        int transactionCount = 0;
        int eventIndex = 0;

        System.out.println("Starting event loop...");

        for (TransactionEvent event : events) {
            eventIndex++;
            System.out.println("\n--- Processing event " + eventIndex + "/" + events.size() + " ---");
            System.out.println("    Event type: " + event.getClass().getSimpleName());
            System.out.println("    Event: " + event);

            if (event instanceof Transaction) {
                Transaction tx = (Transaction) event;
                String sender = tx.getSender();
                transactionCount++;

                // Add to current phase
                currentPhaseSenders.computeIfAbsent(sender, k -> new ArrayList<>()).add(tx);

                System.out.println("  → Accumulated transaction #" + transactionCount + " for sender " + sender);
                System.out.println("  → Current phase now has " + countTransactions(currentPhaseSenders) +
                        " transactions from " + currentPhaseSenders.size() + " senders");

            } else if (event instanceof LeaderFailure) {
                leaderFailureCount++;

                System.out.println("\n╔══════════════════════════════════════════════════════════╗");
                System.out.println("║   LEADER FAILURE #" + leaderFailureCount + " at event " + eventIndex + "/" + events.size());
                System.out.println("╚══════════════════════════════════════════════════════════╝");

                // First, spawn threads for all accumulated transactions (if any)
                if (!currentPhaseSenders.isEmpty()) {
                    System.out.println("→ Spawning transaction threads for phase " + leaderFailureCount +
                            " (" + currentPhaseSenders.size() + " senders, " +
                            countTransactions(currentPhaseSenders) + " transactions)");

                    // DEBUG: Show what we're spawning
                    for (Map.Entry<String, List<Transaction>> entry : currentPhaseSenders.entrySet()) {
                        System.out.println("    Sender " + entry.getKey() + ": " + entry.getValue().size() + " transactions");
                    }

                    spawnTransactionThreads(currentPhaseSenders, transactionProcessor, leaderFailureCount);
                    currentPhaseSenders.clear(); // Clear for next phase
                } else {
                    System.out.println("→ No transactions to spawn before this leader failure");
                }

                // Now BLOCK on leader failure handling
                System.out.println("→ Handling leader failure (BLOCKING)...");
                try {
                    leaderFailureHandler.handle(leaderFailureCount);
                    System.out.println("✓ Leader failure #" + leaderFailureCount + " handling completed");
                } catch (Exception e) {
                    System.err.println("✗ Error handling leader failure: " + e.getMessage());
                    throw new ExecutionException(e);
                }

                System.out.println("→ Continuing to next phase...\n");
            } else {
                System.out.println("  WARNING: Unknown event type: " + event.getClass().getName());
            }
        }

        System.out.println("\n=== Event loop completed ===");
        System.out.println("Total events processed: " + eventIndex);
        System.out.println("Transactions accumulated: " + transactionCount);
        System.out.println("Current phase has: " + countTransactions(currentPhaseSenders) + " transactions");

        // CRITICAL: Don't forget to spawn threads for remaining transactions after last event
        if (!currentPhaseSenders.isEmpty()) {
            int finalPhase = leaderFailureCount > 0 ? leaderFailureCount + 1 : 1;
            System.out.println("\n╔══════════════════════════════════════════════════════════╗");
            System.out.println("║ Processing final phase " + finalPhase);
            System.out.println("╚══════════════════════════════════════════════════════════╝");
            System.out.println("→ Spawning transaction threads for final phase");
            System.out.println("  Senders: " + currentPhaseSenders.size());
            System.out.println("  Transactions: " + countTransactions(currentPhaseSenders));

            // DEBUG: Show what we're spawning
            for (Map.Entry<String, List<Transaction>> entry : currentPhaseSenders.entrySet()) {
                System.out.println("    Sender " + entry.getKey() + ": " + entry.getValue().size() + " transactions");
            }

            spawnTransactionThreads(currentPhaseSenders, transactionProcessor, finalPhase);
        } else {
            System.out.println("\n⚠️  WARNING: No transactions left to spawn in final phase!");
        }

        System.out.println("\n╔════════════════════════════════════════════════════════════╗");
        System.out.println("║ All transaction threads spawned for TransactionSet #" + transactionSet.setNumber());
        System.out.println("║ Total transactions processed: " + transactionCount);
        System.out.println("║ Total leader failures: " + leaderFailureCount);
        System.out.println("║ (Transactions may still be processing in background)");
        System.out.println("╚════════════════════════════════════════════════════════════╝\n");
    }

    /**
     * Helper method to count total transactions across all senders
     */
    private int countTransactions(Map<String, List<Transaction>> senderTransactions) {
        return senderTransactions.values().stream()
                .mapToInt(List::size)
                .sum();
    }

    /**
     * Spawn asynchronous threads for all senders in the current phase
     * This method returns immediately without waiting for threads to complete
     *
     * UPDATED: Added delays between submissions to prevent overwhelming the system
     */
    private void spawnTransactionThreads(Map<String, List<Transaction>> senderTransactions,
                                         TransactionProcessor processor,
                                         int phaseNumber) {
        int senderIndex = 0;
        int totalTransactions = countTransactions(senderTransactions);

        System.out.println("  Spawning threads for " + senderTransactions.size() + " senders with " +
                totalTransactions + " total transactions");

        for (Map.Entry<String, List<Transaction>> entry : senderTransactions.entrySet()) {
            senderIndex++;
            String sender = entry.getKey();
            List<Transaction> transactions = entry.getValue();

            // Add a small delay between thread submissions to avoid overwhelming the system
            if (senderIndex > 1) {
                try {
                    Thread.sleep(10); // 50ms delay between sender thread spawns
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Submit task asynchronously - don't wait
            executorService.submit(() -> {
                try {
                    long threadId = Thread.currentThread().getId();
                    String threadName = Thread.currentThread().getName();
                    System.out.println("  [Phase-" + phaseNumber + " Thread-" + threadId + "] " +
                            "Started processing sender: " + sender + " (" + transactions.size() + " txn)");

                    // Process all transactions for this sender
                    processor.process(sender, transactions);

                    System.out.println("  [Phase-" + phaseNumber + " Thread-" + threadId + "] " +
                            "✓ Completed sender: " + sender + " (" + transactions.size() + " txn)");
                } catch (Exception e) {
                    System.err.println("  [Phase-" + phaseNumber + " Thread-" + Thread.currentThread().getId() +
                            "] ✗ Error in sender " + sender + ": " + e.getMessage());
                    e.printStackTrace();
                }
            });

            System.out.println("  → Spawned thread " + senderIndex + "/" + senderTransactions.size() +
                    " for sender " + sender + " (" + transactions.size() + " transactions)");
        }

        System.out.println("  ✓ All " + senderIndex + " sender threads spawned for phase " + phaseNumber);
    }

    /**
     * Shutdown the executor service. Called when done with all processing.
     *
     * IMPORTANT: This waits for all submitted tasks to complete before shutting down.
     */
    public void shutdown() {
        System.out.println("Shutting down TransactionSetExecutor for set #" + transactionSet.setNumber() + "...");

        executorService.shutdown();
        try {
            // Wait up to 2 minutes for all transactions to complete
            if (!executorService.awaitTermination(120, TimeUnit.SECONDS)) {
                System.err.println("Warning: Some transactions did not complete in time for set #" +
                        transactionSet.setNumber());
                List<Runnable> droppedTasks = executorService.shutdownNow();
                System.err.println("Dropped " + droppedTasks.size() + " tasks");
            } else {
                System.out.println("✓ All transactions completed for set #" + transactionSet.setNumber());
            }
        } catch (InterruptedException e) {
            System.err.println("Shutdown interrupted for set #" + transactionSet.setNumber());
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}