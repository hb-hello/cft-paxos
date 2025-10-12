package org.example;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static org.example.ConfigLoader.loadServersFromConfig;

public class ChannelManager {

    private static final Logger logger = LogManager.getLogger(ChannelManager.class);
    private final static HashMap<String, ManagedChannel> channels = new HashMap<>(); // [channels]: Map of server ids and their gRPC channels

    //    Channel management methods

    public static ManagedChannel createChannel(String serverId) {

        Map<String, ServerDetails> servers = Config.getServers();

        if (!servers.containsKey(serverId)) {
            logger.error("Server ID {} not found in configuration while creating GRPC channel.", serverId);
            throw new RuntimeException();
        }
        ServerDetails server = servers.get(serverId);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(server.host(), server.port()).usePlaintext().build();
        channels.put(serverId, channel);
        logger.info("Initialized GRPC channel to server {} at {}:{}", serverId, server.host(), server.port());
        return channel;
    }

    public static void shutdownChannels() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            String serverId = entry.getKey();
            ManagedChannel channel = entry.getValue();
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
                logger.info("Shutdown GRPC channel to server {}", serverId);
            }
        }
    }

    public static ManagedChannel createOrGetChannel(String serverId) {
        if (channels.containsKey(serverId)) {
            return channels.get(serverId);
        }
        return createChannel(serverId);
    }

    public static HashMap<String, ManagedChannel> getChannels(String exceptServerId) {
        for (String serverId : Config.getServerIdsExcept(exceptServerId)) {
            createOrGetChannel(serverId);
        }
        return new HashMap<>(channels);
    }

}
