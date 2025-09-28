package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Client {
    private static final Logger logger = LogManager.getLogger(Client.class);
    private String serverDetailsFilePath = "src/main/resources/serverDetails.json";
    private final String clientId; // c: self client_id
    private HashMap<String, ServerDetails> servers; // [servers]: Map of all server ids and their connection info
    private String leaderId; // leader: Current leader id
    private final HashMap<String, ManagedChannel> channels; // [channels]: Map of server ids and their gRPC channels
    private final HashMap<String, MessageServiceGrpc.MessageServiceBlockingStub> stubs; // [stubs]: Map of server ids and their gRPC stubs
    private final HashMap<String, MessageServiceOuterClass.ClientReply> history; // [history]: Map of old requests sent and replies received

    public Client(String clientId) {
        this.clientId = clientId;
        this.channels = new HashMap<>();
        this.stubs = new HashMap<>();
        this.servers = new HashMap<>();
        this.history = new HashMap<>();

        try {
            this.servers = loadServersFromConfig(serverDetailsFilePath);
        } catch (Exception e) {
            logger.error("Client {}: Failed to load server details from default config file {} : {}", clientId, serverDetailsFilePath, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public Client(String clientId, String serverDetailsFilePath) {
        this.clientId = clientId;
        this.serverDetailsFilePath = serverDetailsFilePath;
        this.channels = new HashMap<>();
        this.stubs = new HashMap<>();
        this.servers = new HashMap<>();
        this.history = new HashMap<>();

        try {
            this.servers = loadServersFromConfig(serverDetailsFilePath);
        } catch (Exception e) {
            logger.error("Client {}: Failed to load server details from user-provided config file {} : {}", clientId, serverDetailsFilePath, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static HashMap<String, ServerDetails> loadServersFromConfig(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<ServerDetails> serverList = mapper.readValue(new File(filePath), new TypeReference<List<ServerDetails>>() {
        });
        Map<String, ServerDetails> servers = new HashMap<>();
        for (ServerDetails server : serverList) {
            servers.put(server.id(), server);
        }
        return new HashMap<>(servers);
    }

    public static String[] loadClientIds(String filePath) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            List<ClientDetails> clientList = mapper.readValue(new File(filePath), new TypeReference<List<ClientDetails>>() {
            });
            String[] clientIds = new String[clientList.size()];
            for (int i = 0; i < clientList.size(); i++) {
                clientIds[i] = clientList.get(i).id();
            }
            return clientIds;
        } catch (Exception e) {
            logger.error("Failed to load client IDs from config file {} : {}", filePath, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public String getClientId() {
        return clientId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    /**
     * Generates a ClientRequest proto message.
     *
     * @param sender    Sender account ID
     * @param receiver  Receiver account ID
     * @param amount    Transaction amount
     * @param timestamp Timestamp for the request
     * @return ClientRequest proto message
     */
    private static MessageServiceOuterClass.ClientRequest generateClientRequest(String sender, String receiver, double amount, long timestamp, String clientId) {
        MessageServiceOuterClass.Transaction transaction = MessageServiceOuterClass.Transaction.newBuilder().setSender(sender).setReceiver(receiver).setAmount(amount).build();
        return MessageServiceOuterClass.ClientRequest.newBuilder().setTransaction(transaction).setTimestamp(timestamp).setClientId(clientId).build();
    }

//    Channel management methods

    private ManagedChannel createChannel(String serverId) {
        if (!servers.containsKey(serverId)) {
            logger.error("Client {}: Server ID {} not found in configuration.", clientId, serverId);
            throw new RuntimeException();
        }
        ServerDetails server = servers.get(serverId);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(server.host(), server.port()).usePlaintext().build();
        channels.put(serverId, channel);
        logger.info("Client {}: Initialized gRPC channel to server {} at {}:{}", clientId, serverId, server.host(), server.port());
        return channel;
    }

    private void shutdownChannels() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            String serverId = entry.getKey();
            ManagedChannel channel = entry.getValue();
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
                logger.info("Client {}: Shutdown gRPC channel to server {}", clientId, serverId);
            }
        }
    }

    private ManagedChannel createOrGetChannel(String serverId) {
        if (channels.containsKey(serverId)) {
            return channels.get(serverId);
        }
        return createChannel(serverId);
    }

//    Stub management methods

    private MessageServiceGrpc.MessageServiceBlockingStub createStub(String serverId) {
        ManagedChannel channel = createOrGetChannel(serverId);
        MessageServiceGrpc.MessageServiceBlockingStub stub = MessageServiceGrpc.newBlockingStub(channel);
        stubs.put(serverId, stub);
        logger.info("Client {}: Initialized gRPC stub for server {}", clientId, serverId);
        return stub;
    }


    /**
     * @param serverId The target server's ID
     * @return The blocking stub for the specified server, creating it if it doesn't already exist
     */
    private MessageServiceGrpc.MessageServiceBlockingStub createOrGetStub(String serverId) {
        if (stubs.containsKey(serverId)) {
            return stubs.get(serverId);
        }
        return createStub(serverId);
    }

    /**
     * Sends a ClientRequest to the specified server using a provided gRPC ManagedChannel, waits for a response with a timeout,
     * and retries if no response is received. Retries up to maxRetries times.
     *
     * @param request       The ClientRequest to send
     * @param serverId      The target server's ID (for logging only)
     * @param timeoutMillis Timeout in milliseconds to wait for a response
     * @param maxRetries    Maximum number of retries if no response is received
     * @return The server's response, or null if all retries fail
     */
    public MessageServiceOuterClass.ClientReply sendClientRequestWithRetry(MessageServiceOuterClass.ClientRequest request, String serverId, long timeoutMillis, int maxRetries) {
        int attempt = 0;
        while (attempt <= maxRetries) {
            attempt++;
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                MessageServiceGrpc.MessageServiceBlockingStub stub = createOrGetStub(serverId);
                logger.info("Client {}: Sending ClientRequest to server {} (attempt {}/{})", clientId, serverId, attempt, maxRetries + 1);
                Future<MessageServiceOuterClass.ClientReply> future = executor.submit(() -> stub.request(request));
                try {
                    MessageServiceOuterClass.ClientReply reply = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                    logger.info("Client {}: Received reply from server {}: {}", clientId, serverId, reply);
                    return reply;
                } catch (TimeoutException e) {
                    logger.warn("Client {}: Timeout waiting for reply from server {} (attempt {}/{}). Retrying...", clientId, serverId, attempt, maxRetries + 1);
                    future.cancel(true);
                } catch (ExecutionException | InterruptedException e) {
                    logger.error("Client {}: Error while waiting for reply from server {}: {}", clientId, serverId, e.getMessage());
                }
            } catch (StatusRuntimeException e) {
                logger.error("Client {}: gRPC error communicating with server {}: {}", clientId, serverId, e.getMessage());
            }
        }
        logger.error("Client {}: Failed to receive reply from server {} after {} attempts.", clientId, serverId, maxRetries + 1);
        return null;
    }

    public static void main(String[] args) {

//        Create multiple threads to simulate multiple clients

        String clientDetailsFilePath = "src/main/resources/clientDetails.json";
        String[] clientIds = loadClientIds(clientDetailsFilePath);

//        Wrap in try-with-resources statement to ensure proper shutdown

        try (ExecutorService executor = Executors.newFixedThreadPool(clientIds.length)) {
            for (final String clientId : clientIds) {
                executor.submit(() -> {
                            try {
                                Client client = new Client(clientId);
                                logger.info("Client {}: Client initialized", client.getClientId());
                                long timestamp = System.currentTimeMillis();
                                MessageServiceOuterClass.ClientRequest request = generateClientRequest(clientId, "B", 10.0, timestamp, client.getClientId());
                                MessageServiceOuterClass.ClientReply reply = client.sendClientRequestWithRetry(request, "1", 500, 2);
                            } catch (Exception e) {
                                logger.error("Error initializing client {} : {}", clientId, e.getMessage());
                            }

                        }
                );
            }
        } catch (Exception e) {
            logger.error("Error in spawning multiple threads for clients : {}", e.getMessage());
        }

    }

}