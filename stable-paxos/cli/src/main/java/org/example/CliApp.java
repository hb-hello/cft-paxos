package org.example;

import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static org.example.ChannelManager.createOrGetChannel;
import static org.example.ConfigLoader.loadServersFromConfig;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class CliApp {

    private static final Logger logger = LogManager.getLogger(CliApp.class);
    private final String serverDetailsFilePath;
    private final String clientDetailsFilePath;
    private final String transactionSetsFilePath;
    private static final long TIMEOUT_MILLIS = 500;
    private static final int MAX_RETRIES = 5;
    private final Map<String, ServerNode> serverNodes;
    private final Map<Integer, TransactionSet> transactionSets;

    public CliApp(String serverDetailsFilePath, String clientDetailsFilePath, String transactionSetsFilePath) {
        this.serverDetailsFilePath = serverDetailsFilePath;
        this.clientDetailsFilePath = clientDetailsFilePath;
        this.transactionSetsFilePath = transactionSetsFilePath;

        this.serverNodes = new HashMap<>();
        initializeAllServers();

        try {
            transactionSets = TransactionSetLoader.loadTransactionSets(transactionSetsFilePath);
        } catch (Exception e) {
            logger.error("Error loading transaction sets: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void initializeAllServers() {
        try {
            Map<String, ServerDetails> servers = loadServersFromConfig(serverDetailsFilePath);
            for (String serverId: servers.keySet()) {
                serverNodes.put(serverId, new ServerNode(serverId));
            }
        } catch (Exception e) {
            logger.error("Error loading server details from config file while starting servers : {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void activateServerNode(String serverId, String serverDetailsFilePath) {
        try {
            ManagedChannel channel = createOrGetChannel(serverId, serverDetailsFilePath);
            MessageServiceGrpc.MessageServiceBlockingStub stub = MessageServiceGrpc.newBlockingStub(channel);
            MessageServiceOuterClass.Acknowledgement ack = stub.setActiveFlag(MessageServiceOuterClass.ActiveFlag.newBuilder().setActiveFlag(true).build());
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

//        TODO: Start all server processes

        final String SERVER_DETAILS_FILE_PATH = "src/main/resources/serverDetails.json";
        final String CLIENT_DETAILS_FILE_PATH = "src/main/resources/clientDetails.json";
        final String TRANSACTION_SETS_FILE_PATH = "src/main/resources/transactionSets.csv";

        CliApp cli = new CliApp(SERVER_DETAILS_FILE_PATH, CLIENT_DETAILS_FILE_PATH, TRANSACTION_SETS_FILE_PATH);

            for (int setNumber : cli.transactionSets.keySet()) {

                TransactionSet transactionSet = cli.transactionSets.get(setNumber);

//                Activate required nodes
                for (String serverIdToActivate: transactionSet.activeNodesList()) {
                    activateServerNode(serverIdToActivate, SERVER_DETAILS_FILE_PATH);
                }

//                try (ExecutorService executor = Executors.newFixedThreadPool(transactionSet.transactions().size())) {
////                  Loop through all senders in the transaction set
////                  Key in the transactions map represents the sender
//                    for (String clientId : transactionSet.transactions().keySet()) {
//                        executor.submit(() -> {
//                            try {
//                                Client client = new Client(clientId);
//                                logger.info("Client {} initialized.", clientId);
//                                for (MessageServiceOuterClass.Transaction transaction : transactionSet.transactions().get(clientId)) {
//                                    client.processTransaction(transaction, TIMEOUT_MILLIS, MAX_RETRIES);
//                                }
//                            } catch (Exception e) {
//                                throw new RuntimeException(e);
//                            }
//                        });
//                    }
//                }
            }
    }
}