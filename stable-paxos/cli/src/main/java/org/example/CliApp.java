package org.example;

import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

import static org.example.ChannelManager.createOrGetChannel;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class CliApp {

    private static final Logger logger = LogManager.getLogger(CliApp.class);
    private final Map<Integer, TransactionSet> transactionSets;

    public CliApp() {
        try {
            transactionSets = TransactionSetLoader.loadTransactionSets(Config.getTransactionSetsPath());
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

    private static void activateServers(TransactionSet transactionSet) {
//        Deactivate all servers
        for (String serverId: Config.getServers().keySet()) {
            setServerNodeActiveFlag(serverId, false);
        }

//        Activate required servers based on transaction set
        for (String serverIdToActivate : transactionSet.activeNodesList()) {
            setServerNodeActiveFlag(serverIdToActivate, true);
        }
    }

    private static void startClients(TransactionSet transactionSet) {
        List<Callable<Void>> clients = getClientCallables(transactionSet);

        try (ExecutorService executor = Executors.newFixedThreadPool(transactionSet.transactions().size())) {
                List<Future<Void>> results = executor.invokeAll(clients);

                // Optional: Check for any execution exceptions
                for (Future<Void> future : results) {
                    try {
                        future.get(); // This will rethrow exceptions from within the Callable
                    } catch (ExecutionException e) {
                        logger.error("Error during client execution: {}", e.getCause().getMessage(), e.getCause());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // restore interrupt status
                logger.error("Client execution was interrupted: {}", e.getMessage(), e);
            }
    }

    private static List<Callable<Void>> getClientCallables(TransactionSet transactionSet) {
        List<Callable<Void>> clients = new ArrayList<>();

        for (String clientId : transactionSet.transactions().keySet()) {
            clients.add(() -> {
                try {
                    Client client = new Client(clientId);
                    logger.info("Client {} initialized.", clientId);

                    for (MessageServiceOuterClass.Transaction transaction : transactionSet.transactions().get(clientId)) {
                        client.processTransaction(transaction);
                    }

                    return null; // Callable<Void> requires a return
                } catch (Exception e) {
                    logger.error("Client {} encountered an error: {}", clientId, e.getMessage(), e);
                    throw e;
                }
            });
        }
        return clients;
    }

    public static void main(String[] args) {

        Config.initialize();

        CliApp cli = new CliApp();

//        Start all servers
//        ServerManager.startAllServerProcesses();
//        ServerManager.registerShutdownHook();

//        Starting all servers using IDE

        Scanner scanner = new Scanner(System.in);
        System.out.print("\n\nStart with transaction set : ");
        int startingSetNumber = Integer.parseInt(scanner.next());
        System.out.println();

        for (int setNumber = startingSetNumber; setNumber < cli.transactionSets.size(); setNumber++) {

            logger.info("Loading transaction set {}...", setNumber);
            TransactionSet transactionSet = cli.transactionSets.get(setNumber);

            System.out.println(transactionSet);

            logger.info("Activating server nodes");
            activateServers(transactionSet);

            logger.info("Starting clients...");
            startClients(transactionSet);

            System.out.println("Completed set 1.\n\nWhat operation do you want to perform?");
            System.out.println("1 - Continue with next set");
            System.out.print("Enter a number to perform the corresponding operation - ");
            int menuOption = Integer.parseInt(scanner.next());

//            #TODO: Menu of functions to call after each transaction set
        }
    }
}