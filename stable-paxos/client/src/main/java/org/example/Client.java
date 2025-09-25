package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

public class Client {
    private static final Logger logger = LogManager.getLogger(Client.class);
    private String filePath = "src/main/resources/serverDetails.json";
    private final String clientId; // c: self client_id
    private final HashMap<String, ServerDetails> servers; // [servers]: Map of all server ids and their connection info
    private String leaderId; // leader: Current leader id
    private final Timer timer; // timer: Timer for client timeout
//    private final Map<String, Reply> history; // [history]: Map of old requests sent and replies received

    public Client(String clientId) {
        this.clientId = clientId;
        this.timer = new Timer();
//        this.history = new java.util.HashMap<>();
        try {
            this.servers = loadServersFromConfig(filePath);
        } catch (Exception e) {
            logger.error("Failed to load server details from default config file: {}", filePath, e);
            throw new RuntimeException(e);
        }
    }

    public Client(String clientId, String filePath) {
        this.clientId = clientId;
        this.filePath = filePath;
        this.timer = new Timer();
//        this.history = new java.util.HashMap<>();
        try {
            this.servers = loadServersFromConfig(filePath);
        } catch (Exception e) {
            logger.error("Failed to load server details from user-provided config file: {}", filePath, e);
            throw new RuntimeException(e);
        }
    }

    public static HashMap<String, ServerDetails> loadServersFromConfig(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<ServerDetails> serverList = mapper.readValue(new File(filePath), new TypeReference<List<ServerDetails>>() {
        });
        Map<String, ServerDetails> servers = new HashMap<>();
        for (ServerDetails server : serverList) {
            servers.put(server.id(), server);
        }
        return new HashMap<>(servers);
    }

    public String getClientId() {
        return clientId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    /**
     * Generates a ClientRequest proto message.
     * @param sender Sender account ID
     * @param receiver Receiver account ID
     * @param amount Transaction amount
     * @param timestamp Timestamp for the request
     * @return ClientRequest proto message
     */
    public static MessageServiceOuterClass.ClientRequest generateClientRequest(String sender, String receiver, double amount, long timestamp, String clientId) {
        MessageServiceOuterClass.Transaction transaction = MessageServiceOuterClass.Transaction.newBuilder()
                .setSender(sender)
                .setReceiver(receiver)
                .setAmount(amount)
                .build();
        return MessageServiceOuterClass.ClientRequest.newBuilder()
                .setTransaction(transaction)
                .setTimestamp(timestamp)
                .setClientId(clientId)
                .build();
    }

    public static void main(String[] args) {
        try {
            Client client = new Client("1");
            System.out.println("Client initialized with ID: " + client.getClientId());
            // Further client operations can be added here
        } catch (Exception e) {
            logger.error("Error initializing client", e);
        }
    }

}