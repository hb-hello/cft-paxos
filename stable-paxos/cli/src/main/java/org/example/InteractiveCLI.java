package org.example;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;

import java.util.*;
import java.util.concurrent.*;

import static org.example.ChannelManager.createOrGetChannel;

public class InteractiveCLI {

    private final Scanner scanner;
    private final HashMap<Integer, TransactionSet> transactionSets;
    private final ExecutorService backgroundExecutor;
    private final List<TransactionSetExecutor> activeExecutors; // Track all executors
    private boolean shutdownRequested = false;

    public InteractiveCLI(String csvFilePath) {
        this.scanner = new Scanner(System.in);
        this.transactionSets = TransactionSetLoader.loadTransactionSets(csvFilePath);
        this.backgroundExecutor = Executors.newCachedThreadPool(); // Changed to cached to handle multiple sets
        this.activeExecutors = Collections.synchronizedList(new ArrayList<>()); // Thread-safe list

        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║     Multi-Paxos Distributed Banking System CLI           ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Loaded " + transactionSets.size() + " transaction sets from CSV");
        System.out.println();
    }

    /**
     * Main execution loop - processes transaction sets sequentially
     */
    public void run() throws InterruptedException, ExecutionException {
        // Get sorted list of set numbers
        List<Integer> setNumbers = new ArrayList<>(transactionSets.keySet());
        Collections.sort(setNumbers);

        for (int i = 0; i < setNumbers.size(); i++) {
            if (shutdownRequested) {
                System.out.println("Shutdown requested. Exiting...");
                break;
            }

            Integer setNumber = setNumbers.get(i);
            TransactionSet currentSet = transactionSets.get(setNumber);
            boolean isLastSet = (i == setNumbers.size() - 1);
            processTransactionSet(currentSet, isLastSet);
        }

        if (!shutdownRequested) {
            System.out.println("\n" + "═".repeat(60));
            System.out.println("All transaction sets completed successfully!");
            System.out.println("═".repeat(60));

            // Wait for ALL executors to finish their work
            System.out.println("\nWaiting for all background transactions to complete...");
            System.out.println("Active executors: " + activeExecutors.size());

            // Shutdown all transaction set executors first
            for (TransactionSetExecutor executor : activeExecutors) {
                System.out.println("Shutting down executor for set...");
                executor.shutdown();
            }

            // Now shutdown the background executor
            backgroundExecutor.shutdown();

            if (backgroundExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("✓ All background transactions completed");
            } else {
                System.out.println("⚠️  Warning: Some transactions may still be running");
                backgroundExecutor.shutdownNow();
            }

            // Give a bit more time for final transactions to complete
            System.out.println("Waiting additional time for final transactions...");
            Thread.sleep(2000);
        }

        scanner.close();
    }

    /**
     * Process a single transaction set with user confirmation and menu
     */
    private void processTransactionSet(TransactionSet set, boolean isLastSet) throws InterruptedException, ExecutionException {
        System.out.println("\n" + "═".repeat(60));
        System.out.println("Transaction Set #" + set.setNumber() + (isLastSet ? " (LAST SET)" : ""));
        System.out.println("═".repeat(60));
        System.out.println("Active Nodes: " + set.activeNodesList());
        System.out.println("Total Events: " + set.transactionEvents().size());

        // Show preview of events
        System.out.println("\nEvents Preview:");
        int eventCount = 0;
        for (TransactionEvent event : set.transactionEvents()) {
            eventCount++;
            if (eventCount <= 5) {
                System.out.println("  " + eventCount + ". " + event);
            } else if (eventCount == 6) {
                System.out.println("  ... (" + (set.transactionEvents().size() - 5) + " more events)");
                break;
            }
        }

        System.out.println();

        // Ask user to start this set
        System.out.print("Press ENTER to start Transaction Set #" + set.setNumber() + " or type 'skip' to skip: ");
        System.out.flush();
        String input = scanner.nextLine().trim();

        if (input.equalsIgnoreCase("skip")) {
            System.out.println("Skipped Transaction Set #" + set.setNumber());
            return;
        }

        // Activate specified server nodes
        ServerManager.activateServers(set);

        // Execute the transaction set asynchronously
        executeTransactionSet(set);

        // Show interactive menu after transactions are started
        showMenuLoop(set, isLastSet);
    }

    /**
     * Execute the transaction set using TransactionSetExecutor
     * FIXED: Now tracks the executor and ensures proper shutdown
     */
    private void executeTransactionSet(TransactionSet set) throws InterruptedException, ExecutionException {
        System.out.println("\n>>> Starting execution of Transaction Set #" + set.setNumber() + " <<<\n");
        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║  SUBMITTING SET #" + set.setNumber() + " TO EXECUTOR  ║");
        System.out.println("╚════════════════════════════════════════╝");

        backgroundExecutor.submit(() -> {
            TransactionSetExecutor executor = null;
            try {
                executor = new TransactionSetExecutor(set, 20);

                // CRITICAL: Track this executor so we can shut it down properly later
                activeExecutors.add(executor);

                // Define transaction processor
                TransactionSetExecutor.TransactionProcessor processor = (sender, transactions) -> {
                    System.out.println("  [Thread-" + Thread.currentThread().getId() + "] Processing sender: " + sender +
                            " (" + transactions.size() + " transaction(s))");

                    Client client = new Client(sender);

                    for (int i = 0; i < transactions.size(); i++) {
                        Transaction tx = transactions.get(i);
                        System.out.println("    → [" + (i+1) + "/" + transactions.size() + "] " + tx);

                        try {
                            client.processTransaction(tx.toProtoTransaction());

                            // Add small delay between transactions from same sender to prevent overwhelming
                            if (i < transactions.size() - 1) {
                                Thread.sleep(100); // 100ms between transactions
                            }
                        } catch (Exception e) {
                            System.err.println("    ✗ Transaction failed: " + tx + " - " + e.getMessage());
                        }
                    }

                    System.out.println("  [Thread-" + Thread.currentThread().getId() + "] ✓ Finished all " +
                            transactions.size() + " transaction(s) for sender: " + sender);
                };

                // Define leader failure handler
                TransactionSetExecutor.LeaderFailureHandler failureHandler = (eventPosition) -> {
                    System.out.println("\n╔══════════════════════════════════════╗");
                    System.out.println("║            LEADER FAILURE ️             ║");
                    System.out.println("╚══════════════════════════════════════╝");

                    Thread.sleep(200);
                    Client.broadcastFailLeader();
                };

                // Execute events
                executor.executeEvents(processor, failureHandler);

                System.out.println("\n✓ Transaction Set #" + set.setNumber() + " execution completed");

            } catch (InterruptedException e) {
                System.err.println("Transaction set #" + set.setNumber() + " was interrupted");
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                System.err.println("Error executing transaction set #" + set.setNumber() + ": " + e.getMessage());
                e.printStackTrace();
            } finally {
                // Note: We don't shutdown here anymore - we'll shutdown all executors at the end
                // This allows the last set's transactions to complete
                if (executor != null) {
                    System.out.println("Transaction set #" + set.setNumber() + " wrapper task finished");
                }
            }
        });

        // Don't wait for the background task to complete
        Thread.sleep(200);
        System.out.println("Transaction threads spawned. You can now interact with the system.\n");
    }

    /**
     * Show interactive menu and handle user commands
     */
    private void showMenuLoop(TransactionSet set, boolean isLastSet) {
        boolean continueToNextSet = false;

        while (!continueToNextSet && !shutdownRequested) {
            printMenu(isLastSet);

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1":
                    handlePrintLog();
                    break;

                case "2":
                    handlePrintDB();
                    break;

                case "3":
                    handlePrintStatus();
                    break;

                case "4":
                    handlePrintView();
                    break;

                case "5":
                    continueToNextSet = true;
                    if (isLastSet) {
                        System.out.println("\nThis was the last transaction set.");
                        System.out.println("Transactions may still be processing in background.");
                        System.out.println("The system will wait for all transactions to complete before exiting.\n");
                    } else {
                        System.out.println("\nContinuing to next transaction set...");
                        System.out.println("(Note: Previous transaction set may still be running in background)\n");
                    }
                    break;

                case "6":
                    handleShutdown();
                    shutdownRequested = true;
                    continueToNextSet = true;
                    break;

                default:
                    System.out.println("Invalid option. Please enter a number between 1 and 6.\n");
            }
        }
    }

    /**
     * Print the interactive menu
     */
    private void printMenu(boolean isLastSet) {
        System.out.println("┌─────────────────────────────────────────────────┐");
        System.out.println("│              COMMAND MENU                       │");
        System.out.println("├─────────────────────────────────────────────────┤");
        System.out.println("│  1. PrintLog     - View server log              │");
        System.out.println("│  2. PrintDB      - View database state          │");
        System.out.println("│  3. PrintStatus  - View transaction status      │");
        System.out.println("│  4. PrintView    - View current view            │");
        if (isLastSet) {
            System.out.println("│  5. Finish       - Complete all processing     │");
        } else {
            System.out.println("│  5. Continue     - Move to next transaction set │");
        }
        System.out.println("│  6. Shutdown     - Exit the system              │");
        System.out.println("└─────────────────────────────────────────────────┘");
        System.out.print("Select an option (1-6): ");
        System.out.flush();
    }

    /**
     * Handle PrintLog command - requires server ID parameter
     */
    private void handlePrintLog() {
        System.out.print("\nEnter Server ID (e.g., n1, n2, n3, etc.): ");
        String serverId = scanner.nextLine().trim();

        if (serverId.isEmpty()) {
            System.out.println("Error: Server ID cannot be empty.\n");
            return;
        }

        System.out.println("\n" + "─".repeat(50));
        System.out.println("LOG FOR SERVER: " + serverId);
        System.out.println("─".repeat(50));

        printLog(serverId);

        System.out.println("─".repeat(50) + "\n");
    }

    /**
     * Handle PrintDB command
     */
    private void handlePrintDB() {
        System.out.println("\n" + "─".repeat(50));
        System.out.println("DATABASE STATE");
        System.out.println("─".repeat(50));

        printDB();

        System.out.println("─".repeat(50) + "\n");
    }

    /**
     * Handle PrintStatus command - requires sequence number parameter
     */
    private void handlePrintStatus() {
        System.out.print("\nEnter Sequence Number: ");
        String input = scanner.nextLine().trim();

        try {
            int sequenceNumber = Integer.parseInt(input);

            System.out.println("\n" + "─".repeat(50));
            System.out.println("STATUS FOR SEQUENCE NUMBER: " + sequenceNumber);
            System.out.println("─".repeat(50));

            printStatus(sequenceNumber);

            System.out.println("─".repeat(50) + "\n");

        } catch (NumberFormatException e) {
            System.out.println("Error: Invalid sequence number. Please enter a valid integer.\n");
        }
    }

    /**
     * Handle PrintView command
     */
    private void handlePrintView() {
        System.out.println("\n" + "─".repeat(50));
        System.out.println("CURRENT VIEW");
        System.out.println("─".repeat(50));

        printView();

        System.out.println("─".repeat(50) + "\n");
    }

    /**
     * Handle Shutdown command
     */
    private void handleShutdown() {
        System.out.println("\n╔═══════════════════════════════════════╗");
        System.out.println("║     Initiating System Shutdown...     ║");
        System.out.println("╚═══════════════════════════════════════╝");

        shutdown();

        System.out.println("System shutdown complete.\n");
    }

    // ==================== STUB METHODS - TO BE IMPLEMENTED ====================

    private void printLog(String serverId) {
        try {
            MessageServiceOuterClass.CLIResponse response = createStub(serverId).getLog(Empty.getDefaultInstance());
            System.out.println(response.getCliResponse());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void printDB() {
        for (String serverId : Config.getServerIds()) {
            System.out.println("Database for server : " + serverId);
            try {
                MessageServiceOuterClass.CLIResponse response = createStub(serverId).getDB(Empty.getDefaultInstance());
                System.out.println(response.getCliResponse());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println();
        }
    }

    private void printStatus(int sequenceNumber) {
        for (String serverId : Config.getServerIds()) {
            System.out.print("Status for sequence number : " + sequenceNumber + " at server : " + serverId + " is ");
            try {
                MessageServiceOuterClass.SequenceNumber seqNumMessage = MessageServiceOuterClass.SequenceNumber.newBuilder()
                        .setSequenceNumber(sequenceNumber)
                        .build();
                MessageServiceOuterClass.CLIResponse response = createStub(serverId).getStatus(seqNumMessage);
                System.out.print(response.getCliResponse() + "\n");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println();
        }
    }

    private void printView() {
        for (String serverId : Config.getServerIds()) {
            try {
                MessageServiceOuterClass.CLIResponse response = createStub(serverId).getNewViews(Empty.getDefaultInstance());
                System.out.print(response.getCliResponse() + "\n");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println();
        }
    }

    private MessageServiceGrpc.MessageServiceBlockingStub createStub(String serverId) {
        ManagedChannel channel = createOrGetChannel(serverId);
        return MessageServiceGrpc.newBlockingStub(channel);
    }

    private void shutdown() {
        System.out.println("Shutting down all transaction executors...");
        for (TransactionSetExecutor executor : activeExecutors) {
            executor.shutdown();
        }

        System.out.println("Shutting down background executor...");
        backgroundExecutor.shutdownNow();

        System.out.println("Closing channels...");
//        ChannelManager.shutdownAll();
    }

    // ==================== MAIN METHOD ====================

    public static void main(String[] args) {
        Config.initialize();
        try {
            String csvPath = args.length > 0 ? args[0] : Config.getTransactionSetsPath();
            InteractiveCLI cli = new InteractiveCLI(csvPath);
            cli.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}