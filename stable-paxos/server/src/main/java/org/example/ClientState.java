package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.example.StateLoader.saveState;

public class ClientState {

    private static final Logger logger = LogManager.getLogger(ClientState.class);
    private final ExecutorService saveExecutor;

    private final Map<String, StateEntry> clientState;
    private final String serverId;  // the JSON file with client IDs and starting bank balances

    public ClientState(String serverId) {
        this.clientState = new ConcurrentHashMap<>(); // concurrent keeps it thread-safe
        this.serverId = serverId;
        this.saveExecutor = Executors.newSingleThreadExecutor();
        this.initialize();
    }

    private void initialize() {
        try {
            Map<String, Double> clientDetails = Config.getClientBalances();
            for (Map.Entry<String, Double> client : clientDetails.entrySet()) {
                StateEntry stateEntry = new StateEntry(client.getKey(), client.getValue());
                this.clientState.put(client.getKey(), stateEntry);
            }
        } catch (Exception e) {
            logger.error("Server {}: Error when loading client details to initialize state : {}", serverId, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void save() {
        try {
            saveState(serverId, clientState);
        } catch (IOException e) {
            logger.error("Server {}: Error saving state to JSON file : {}", serverId, e.getMessage());
            throw new RuntimeException(e);
        }
    }

//    private void getOrInitialize() {
//        try {
//            if (loadState(serverId) == null) {
//                this.initialize();
//        } catch (IOException e) {
//            logger.error("Server {}: Error loading state from JSON file : {}", serverId, e.getMessage());
//            throw new RuntimeException(e);
//        }
//    }

    public long getTimestamp(String clientId) {
        if (clientState.containsKey(clientId)) {
            return clientState.get(clientId).getLastTimestampRepliedTo();
        } else {
            logger.error("Client ID not found while checking timestamp");
            return 0L;
        }

    }

    public void setTimestamp(String clientId, long timestamp) {
        if (clientState.containsKey(clientId)) {
            clientState.get(clientId).setLastTimestampRepliedTo(timestamp);
            logger.info("Timestamp for client ID : {} updated to {}", clientId, timestamp);
            saveExecutor.submit(this::save);  // Save after updating timestamp
        } else {
            logger.error("Client ID not found while setting timestamp");
        }
    }

    public double getBankBalance(String clientId) {
        if (clientState.containsKey(clientId)) {
            return clientState.get(clientId).getBankBalance();
        } else {
            logger.error("Client ID not found while checking bank balance");
            return 0.0;
        }
    }

    public boolean addToBalance(String clientId, double amount) {
        if (clientState.containsKey(clientId)) {
            clientState.get(clientId).addToBankBalance(amount);
            return true;
        } else {
            logger.error("Client ID not found while adding bank balance");
            return false;
        }
    }

    public boolean subtractFromBalance(String clientId, double amount) {
        if (clientState.containsKey(clientId)) {
            return clientState.get(clientId).subtractFromBankBalance(amount);
        } else {
            logger.error("Client ID not found while subtracting bank balance");
            return false;
        }
    }

    public boolean transferBalance(MessageServiceOuterClass.Transaction transaction) {
        boolean deductedFromSender = subtractFromBalance(transaction.getSender(), transaction.getAmount());
        if (deductedFromSender) {
            boolean success = addToBalance(transaction.getReceiver(), transaction.getAmount());
            if (success) {
                saveExecutor.submit(this::save);  // Save after successful transfer
            }
            return success;
        } else return false;
    }

    public void close() {
        saveExecutor.shutdown();
    }

}
