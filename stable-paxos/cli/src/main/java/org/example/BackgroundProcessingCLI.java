package org.example;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.ChannelManager.createOrGetChannel;

/**
 * CLI with one dedicated thread per sender (A-J)
 */
public class BackgroundProcessingCLI {

    private final Scanner scanner;
    private final HashMap<Integer, TransactionSet> transactionSets;
    private final Map<String, ExecutorService> senderExecutors; // One executor per sender
    private final ExecutorService mainExecutor; // For running entire sets
    private final Map<Integer, TransactionSetProgress> progressTracker;

    private static class TransactionSetProgress {
        final int setNumber;
        final AtomicInteger totalTransactions = new AtomicInteger(0);
        final AtomicInteger completedTransactions = new AtomicInteger(0);
        final AtomicInteger failedTransactions = new AtomicInteger(0);
        final AtomicInteger leaderFailures = new AtomicInteger(0);
        volatile boolean isComplete = false;

        TransactionSetProgress(int setNumber) {
            this.setNumber = setNumber;
        }

        @Override
        public String toString() {
            return String.format("Set #%d: %d/%d completed, %d failed, %d LF, %s",
                    setNumber, completedTransactions.get(), totalTransactions.get(),
                    failedTransactions.get(), leaderFailures.get(),
                    isComplete ? "COMPLETE" : "RUNNING");
        }
    }

    public BackgroundProcessingCLI(String csvFilePath) {
        this.scanner = new Scanner(System.in);
        this.transactionSets = TransactionSetLoader.loadTransactionSets(csvFilePath);
        this.progressTracker = new ConcurrentHashMap<>();

        // Create one single-threaded executor per sender (A-J)
        this.senderExecutors = new HashMap<>();
        for (char sender = 'A'; sender <= 'J'; sender++) {
            String senderId = String.valueOf(sender);
            ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r);
                t.setName("Sender-" + senderId);
                t.setDaemon(true);
                return t;
            });
            senderExecutors.put(senderId, executor);
        }

        // Main executor for running transaction sets
        this.mainExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║   Multi-Paxos Banking - Background Processing CLI        ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        System.out.println("Loaded " + transactionSets.size() + " transaction sets");
        System.out.println("Created dedicated threads for senders: A-J\n");
    }

    public void run() throws InterruptedException {
        List<Integer> setNumbers = new ArrayList<>(transactionSets.keySet());
        Collections.sort(setNumbers);

        for (int i = 0; i < setNumbers.size(); i++) {
            Integer setNumber = setNumbers.get(i);
            TransactionSet set = transactionSets.get(setNumber);
            boolean isLastSet = (i == setNumbers.size() - 1);

            processTransactionSet(set, isLastSet);
        }

        System.out.println("\n" + "═".repeat(60));
        System.out.println("All transaction sets initiated!");
        System.out.println("═".repeat(60));

        System.out.println("\nWaiting for all processing to complete...");

        // Shutdown all executors
        mainExecutor.shutdown();
        for (ExecutorService executor : senderExecutors.values()) {
            executor.shutdown();
        }

        if (mainExecutor.awaitTermination(300, TimeUnit.SECONDS)) {
            System.out.println("\n✓ All transactions completed!");
            handlePrintProgress();
        } else {
            System.out.println("\n⚠ Timeout");
        }

        scanner.close();
    }

    private void processTransactionSet(TransactionSet set, boolean isLastSet) throws InterruptedException {
        System.out.println("\n" + "═".repeat(60));
        System.out.println("Transaction Set #" + set.setNumber() + (isLastSet ? " (LAST)" : ""));
        System.out.println("═".repeat(60));
        System.out.println("Active Nodes: " + set.activeNodesList());
        System.out.println("Total Events: " + set.transactionEvents().size());

        System.out.print("\nPress ENTER to start or 'skip': ");
        String input = scanner.nextLine().trim();
        if (input.equalsIgnoreCase("skip")) {
            System.out.println("Skipped Set #" + set.setNumber());
            return;
        }

        ServerManager.activateServers(set);
        Thread.sleep(200);

        startBackgroundProcessing(set);
        showMenu(set, isLastSet);
    }

    private void startBackgroundProcessing(TransactionSet set) {
        TransactionSetProgress progress = new TransactionSetProgress(set.setNumber());
        progressTracker.put(set.setNumber(), progress);

        for (TransactionEvent event : set.transactionEvents()) {
            if (event instanceof Transaction) {
                progress.totalTransactions.incrementAndGet();
            }
        }

        System.out.println("\n>>> Starting Set #" + set.setNumber() + " <<<\n");

        mainExecutor.submit(() -> {
            try {
                processEventsSequentially(set, progress);
            } catch (Exception e) {
                System.err.println("Error in set #" + set.setNumber() + ": " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    /**
     * Process events sequentially, using dedicated sender executors
     * - Each sender's transactions go to their dedicated thread
     * - Leader failures block the main event processing
     */
    private void processEventsSequentially(TransactionSet set, TransactionSetProgress progress)
            throws InterruptedException {

        System.out.println("[BG] Processing Set #" + set.setNumber());
        System.out.println("[BG] Total events: " + set.transactionEvents().size());

        // Keep one Client per sender
        Map<String, Client> clients = new HashMap<>();

        int eventNum = 0;
        for (TransactionEvent event : set.transactionEvents()) {
            eventNum++;

            if (event instanceof Transaction) {
                Transaction tx = (Transaction) event;
                String sender = tx.getSender();

                System.out.println("[BG] Event " + eventNum + ": Transaction from " + sender + " → " + tx);

                // Get or create Client for this sender
                Client client = clients.computeIfAbsent(sender, s -> new Client(s));

                // Get sender's dedicated executor
                ExecutorService senderExecutor = senderExecutors.get(sender);

                if (senderExecutor == null) {
                    System.err.println("[BG] ERROR: No executor for sender " + sender);
                    continue;
                }

                // Submit to sender's dedicated thread
                // All transactions from this sender will execute in order on this thread
                senderExecutor.submit(() -> {
                    try {
                        System.out.println("  [" + sender + "] Processing: " + tx);
                        client.processTransaction(tx.toProtoTransaction());
                        progress.completedTransactions.incrementAndGet();
                        Thread.sleep(100);
                        System.out.println("  [" + sender + "] ✓ Completed: " + tx);
                    } catch (Exception e) {
                        progress.failedTransactions.incrementAndGet();
                        System.err.println("  [" + sender + "] ✗ Failed: " + tx + " - " + e.getMessage());
                    }
                });

            } else if (event instanceof LeaderFailure) {
                progress.leaderFailures.incrementAndGet();

                System.out.println("[BG] Event " + eventNum + ": LEADER FAILURE #" + progress.leaderFailures.get());
                System.out.println("[BG] ╔════════════════════════════════╗");
                System.out.println("[BG] ║   HANDLING LEADER FAILURE      ║");
                System.out.println("[BG] ╚════════════════════════════════╝");

                // Handle leader failure SYNCHRONOUSLY (blocks this thread)
                try {
                    Thread.sleep(300);
                    Client.broadcastFailLeader();
                    Thread.sleep(500);
                    System.out.println("[BG] ✓ Leader failure handled\n");
                } catch (Exception e) {
                    System.err.println("[BG] Error handling LF: " + e.getMessage());
                }
            }
        }

        progress.isComplete = true;
        System.out.println("\n[BG] ✓ Set #" + set.setNumber() + " COMPLETE");
        System.out.println("[BG] " + progress + "\n");
    }

    private void showMenu(TransactionSet set, boolean isLastSet) {
        while (true) {
            printMenu(isLastSet);
            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1": handlePrintLog(); break;
                case "2": handlePrintDB(); break;
                case "3": handlePrintStatus(); break;
                case "4": handlePrintView(); break;
                case "5": handlePrintProgress(); break;
                case "6":
                    System.out.println(isLastSet ? "\nFinishing..." : "\nContinuing...");
                    return;
                case "7":
                    System.out.println("\nShutting down...");
                    mainExecutor.shutdownNow();
                    for (ExecutorService ex : senderExecutors.values()) {
                        ex.shutdownNow();
                    }
                    System.exit(0);
                default:
                    System.out.println("Invalid option.\n");
            }
        }
    }

    private void printMenu(boolean isLastSet) {
        System.out.println("┌─────────────────────────────────────────────────┐");
        System.out.println("│              COMMAND MENU                       │");
        System.out.println("├─────────────────────────────────────────────────┤");
        System.out.println("│  1. PrintLog     │  2. PrintDB                  │");
        System.out.println("│  3. PrintStatus  │  4. PrintView                │");
        System.out.println("│  5. Progress     │  6. " + (isLastSet ? "Finish " : "Continue") + "                │");
        System.out.println("│  7. Shutdown                                    │");
        System.out.println("└─────────────────────────────────────────────────┘");
        System.out.print("Select (1-7): ");
        System.out.flush();
    }

    private void handlePrintLog() {
        System.out.print("\nServer ID: ");
        String serverId = scanner.nextLine().trim();
        if (serverId.isEmpty()) return;
        System.out.println("\n" + "─".repeat(50));
        try {
            MessageServiceOuterClass.CLIResponse r = createStub(serverId).getLog(Empty.getDefaultInstance());
            System.out.println(r.getCliResponse());
        } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        System.out.println("─".repeat(50) + "\n");
    }

    private void handlePrintDB() {
        System.out.println("\n" + "─".repeat(50));
        System.out.println("DATABASE STATE");
        System.out.println("─".repeat(50));
        for (String sid : Config.getServerIds()) {
            System.out.println("Server " + sid + ":");
            try {
                MessageServiceOuterClass.CLIResponse r = createStub(sid).getDB(Empty.getDefaultInstance());
                System.out.println(r.getCliResponse());
            } catch (Exception e) { System.err.println("Error: " + e.getMessage()); }
        }
        System.out.println("─".repeat(50) + "\n");
    }

    private void handlePrintStatus() {
        System.out.print("\nSequence Number: ");
        try {
            int seq = Integer.parseInt(scanner.nextLine().trim());
            System.out.println("\n" + "─".repeat(50));
            for (String sid : Config.getServerIds()) {
                System.out.print(sid + ": ");
                try {
                    MessageServiceOuterClass.SequenceNumber msg = MessageServiceOuterClass.SequenceNumber.newBuilder().setSequenceNumber(seq).build();
                    MessageServiceOuterClass.CLIResponse r = createStub(sid).getStatus(msg);
                    System.out.println(r.getCliResponse());
                } catch (Exception e) { System.err.println("Error"); }
            }
            System.out.println("─".repeat(50) + "\n");
        } catch (NumberFormatException e) { System.out.println("Invalid\n"); }
    }

    private void handlePrintView() {
        System.out.println("\n" + "─".repeat(50));
        for (String sid : Config.getServerIds()) {
            try {
                MessageServiceOuterClass.CLIResponse r = createStub(sid).getNewViews(Empty.getDefaultInstance());
                System.out.println(r.getCliResponse());
            } catch (Exception e) { System.err.println(sid + ": Error"); }
        }
        System.out.println("─".repeat(50) + "\n");
    }

    private void handlePrintProgress() {
        System.out.println("\n" + "─".repeat(60));
        System.out.println("PROGRESS");
        System.out.println("─".repeat(60));
        if (progressTracker.isEmpty()) {
            System.out.println("No active sets");
        } else {
            for (TransactionSetProgress p : progressTracker.values()) {
                System.out.println(p);
            }
        }
        System.out.println("─".repeat(60) + "\n");
    }

    private MessageServiceGrpc.MessageServiceBlockingStub createStub(String serverId) {
        ManagedChannel channel = createOrGetChannel(serverId);
        return MessageServiceGrpc.newBlockingStub(channel);
    }

    public static void main(String[] args) {
        Config.initialize();
        try {
            String csvPath = args.length > 0 ? args[0] : Config.getTransactionSetsPath();
            BackgroundProcessingCLI cli = new BackgroundProcessingCLI(csvPath);
            cli.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
