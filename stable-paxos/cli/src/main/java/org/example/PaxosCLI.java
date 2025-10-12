package org.example;

import com.google.protobuf.Empty;

import java.util.List;
import java.util.Scanner;

import static org.example.ChannelManager.createOrGetChannel;

public final class PaxosCLI {

    private static MessageServiceGrpc.MessageServiceBlockingStub createStub(String serverId) {
        return MessageServiceGrpc.newBlockingStub(createOrGetChannel(serverId));
    }

    private static void printLog(String serverId) {
        try {
            MessageServiceOuterClass.CLIResponse response =
                    createStub(serverId).getLog(Empty.getDefaultInstance());
            System.out.println(response.getCliResponse());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void printDB() {
        for (String serverId : Config.getServerIds()) {
            System.out.println("Database for server : " + serverId);
            try {
                MessageServiceOuterClass.CLIResponse response =
                        createStub(serverId).getDB(Empty.getDefaultInstance());
                System.out.println(response.getCliResponse());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println();
        }
    }

    private static void printStatus(int sequenceNumber) {
        for (String serverId : Config.getServerIds()) {
            System.out.print("Status for sequence number : " + sequenceNumber +
                    " at server : " + serverId + " is ");
            try {
                MessageServiceOuterClass.SequenceNumber seqNumMessage =
                        MessageServiceOuterClass.SequenceNumber.newBuilder()
                                .setSequenceNumber(sequenceNumber)
                                .build();
                MessageServiceOuterClass.CLIResponse response =
                        createStub(serverId).getStatus(seqNumMessage);
                System.out.print(response.getCliResponse() + "\n");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println();
        }
    }

    private static void printView() {
        for (String serverId : Config.getServerIds()) {
            try {
                MessageServiceOuterClass.CLIResponse response =
                        createStub(serverId).getNewViews(Empty.getDefaultInstance());
                System.out.print(response.getCliResponse() + "\n");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println();
        }
    }
    // ===================================================================

    public static void main(String[] args) throws Exception {

        Config.initialize();

//        if (args.length < 1) {
//            System.out.println("Usage: java PaxosCLI <path-to-transaction-sets.csv>");
//            return;
//        }

        // Reuse existing loader the project already has
        List<TransactionSet> sets = TransactionSetLoader.loadTransactionSets(Config.getTransactionSetsPath()).values().stream().toList();

        System.out.println("Loaded " + sets.size() + " transaction sets.");
        try (SenderDispatcher dispatcher = new SenderDispatcher()) {
            int next = 0;
            Scanner sc = new Scanner(System.in);

            while (true) {
                System.out.println();
                System.out.println("Options:");
                System.out.println(" 1 - PrintDB");
                System.out.println(" 2 - PrintLog");
                System.out.println(" 3 - PrintStatus");
                System.out.println(" 4 - PrintView");
                System.out.println(" 5 - Continue with next set");
                System.out.println(" 0 - Exit");
                System.out.print("Choice: ");
                String choice = sc.nextLine().trim();

                switch (choice) {
                    case "1" -> printDB();
                    case "2" -> {
                        System.out.print("Enter server id: ");
                        String serverId = sc.nextLine().trim();
                        printLog(serverId);
                    }
                    case "3" -> {
                        System.out.print("Enter sequence number: ");
                        int seq = Integer.parseInt(sc.nextLine().trim());
                        printStatus(seq);
                    }
                    case "4" -> printView();
                    case "5" -> {
                        if (next >= sets.size()) {
                            System.out.println("No more sets.");
                            break;
                        }
                        TransactionSet set = sets.get(next++);
                        System.out.printf("Scheduling set #%d%n", set.setNumber());

                        ServerManager.activateServers(set);

                        // Submit events exactly in file order
                        for (TransactionEvent ev : set.transactionEvents()) {
                            dispatcher.submit(ev);
                        }
                        System.out.println("Set scheduled; processing continues in background.");
                    }
                    case "0" -> {
                        System.out.println("Exiting...");
                        return;
                    }
                    default -> System.out.println("Unknown choice.");
                }

                // Optional local progress peek
                SenderDispatcher.Status s = dispatcher.snapshotStatus();
                System.out.printf("Progress: submitted=%d completed=%d outstanding=%d%n",
                        s.submitted(), s.completed(), s.outstanding());
            }
        }
    }
}
