package org.example;

import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

import static org.example.ChannelManager.createOrGetChannel;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class CliApp {

    private static final Logger logger = LogManager.getLogger(CliApp.class);
    private final Map<Integer, TransactionSet> transactionSets;
    private final Map<Integer, List<Map<String, List<Transaction>>>> transactionSetsByPhases;

    public CliApp() {
        try {
            this.transactionSets = TransactionSetLoader.loadTransactionSets(Config.getTransactionSetsPath());
            this.transactionSetsByPhases = transactionSets.entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().groupTransactionsBySenderPerPhase()
                    ));
        } catch (Exception e) {
            logger.error("Error loading transaction sets: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void setServerNodeActiveFlag(String serverId, boolean activeFlag) {
        try {
            ManagedChannel channel = createOrGetChannel(serverId);
            MessageServiceGrpc.MessageServiceBlockingStub stub = MessageServiceGrpc.newBlockingStub(channel);
            MessageServiceOuterClass.Acknowledgement ack = stub.setActiveFlag(MessageServiceOuterClass.ActiveFlag.newBuilder().setActiveFlag(activeFlag).build());
            if (!ack.getStatus()) {
                logger.error("Server {} not activated", serverId);
                throw new RuntimeException("Server {} not activated");
            } else logger.info("Server {} activated", serverId);
        } catch (RuntimeException e) {
            logger.error("Error when activating server {}.", serverId);
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {

        Config.initialize();

        CliApp cli = new CliApp();

//        Starting all servers using IDE

        for (int setNumber : cli.transactionSets.keySet()) {
            System.out.println("Transaction Set " + setNumber + ":");
            cli.transactionSets.get(setNumber).printPhases();
            System.out.println();
        }

//        Scanner scanner = new Scanner(System.in);
//        System.out.print("\n\nStart with transaction set : ");
//        int startingSetNumber = Integer.parseInt(scanner.next());
//        System.out.println();
//
//        for (int setNumber = startingSetNumber; setNumber < cli.transactionSets.size(); setNumber++) {
//
//            logger.info("Loading transaction set {}...", setNumber);
//            TransactionSet transactionSet = cli.transactionSets.get(setNumber);
//
//            System.out.println(transactionSet);
//
//            logger.info("Activating server nodes");
//            activateServers(transactionSet);
//
//            logger.info("Starting clients...");
//            startClients(transactionSet);
//
//            System.out.println("Completed set 1.\n\nWhat operation do you want to perform?");
//            System.out.println("1 - Continue with next set");
//            System.out.print("Enter a number to perform the corresponding operation - ");
//            int menuOption = Integer.parseInt(scanner.next());

//            #TODO: Menu of functions to call after each transaction set
    }
}