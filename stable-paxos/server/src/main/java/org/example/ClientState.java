package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.example.ConfigLoader.loadClientDetails;
import static org.example.StateLoader.loadState;
import static org.example.StateLoader.saveState;

public class ClientState {

    private static final Logger logger = LogManager.getLogger(ClientState.class);

    private Map<String, ClientRecord> clientState;
    private final String serverId;
    private final String clientDetailsFilePath;   // the JSON file with client IDs and starting bank balances

    public ClientState(String serverId, String clientDetailsFilePath) {
        this.clientState = new HashMap<>();
        this.serverId = serverId;
        this.clientDetailsFilePath = clientDetailsFilePath;
        this.getOrInitialize();
    }

    private void initialize() {
        try {
            Map<String, Double> clientDetails = loadClientDetails(clientDetailsFilePath);
            for (Map.Entry<String, Double> client: clientDetails.entrySet()) {
                ClientRecord clientRecord = new ClientRecord(client.getKey(), client.getValue());
                this.clientState.put(client.getKey(), clientRecord);
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

}
