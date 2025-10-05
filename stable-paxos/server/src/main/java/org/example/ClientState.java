package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.example.StateLoader.loadState;
import static org.example.StateLoader.saveState;

public class ClientState {

//    #TODO: Make this async - does this need to be async? - this should only be accessed in the execute flow and execute should only be sequential - is execute a while loop listener that keeps checking if executeSequenceNumber is committed

    private static final Logger logger = LogManager.getLogger(ClientState.class);

    private Map<String, StateEntry> clientState;
    private final String serverId;  // the JSON file with client IDs and starting bank balances

    public ClientState(String serverId) {
        this.clientState = new ConcurrentHashMap<>(); // concurrent keeps it thread-safe
        this.serverId = serverId;
        this.getOrInitialize();
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

    private void getOrInitialize() {
        try {
            if (loadState(serverId) == null) {
                this.initialize();
            } else this.clientState = loadState(serverId);
        } catch (IOException e) {
            logger.error("Server {}: Error loading state from JSON file : {}", serverId, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public long getTimestamp(String clientId) {
        if (clientState.containsKey(clientId)) {
            return clientState.get(clientId).getLastTimestampRepliedTo();
        } else {
            logger.error("Client ID not found while checking timestamp");
            return 0L;
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
            double oldBankBalance = clientState.get(clientId).getBankBalance();
            clientState.get(clientId).setBankBalance(oldBankBalance + amount);
            return true;
        } else {
            logger.error("Client ID not found while adding bank balance");
            return false;
        }
    }

    public boolean subtractFromBalance(String clientId, double amount) {
        if (clientState.containsKey(clientId)) {
            double oldBankBalance = clientState.get(clientId).getBankBalance();
            if (amount <= oldBankBalance) {
                clientState.get(clientId).setBankBalance(oldBankBalance - amount);
                return true;
            } else {
                logger.error("Bank balance is less than amount to be transferred");
                return false;
            }
        } else {
            logger.error("Client ID not found while subtracting bank balance");
            return false;
        }
    }

    public boolean transferBalance(MessageServiceOuterClass.Transaction transaction) {
        boolean deductedFromSender = subtractFromBalance(transaction.getSender(), transaction.getAmount());
        if (deductedFromSender) {
            return addToBalance(transaction.getReceiver(), transaction.getAmount());
        } else return false;
    }

}
