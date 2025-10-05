package org.example;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class CommunicationManager {

    private static final Logger logger = LogManager.getLogger(ServerNode.class);

    private final Server server;
    private final ServerActivityInterceptor interceptor;
    private final Map<String, ServerDetails> servers;
    private final String serverId;

    public CommunicationManager(String serverId, MessageService service) {
        this.servers = Config.getServers();
        this.interceptor = new ServerActivityInterceptor();
        this.serverId = serverId;
        this.server = ServerBuilder.forPort(Config.getServerPort(serverId))
                .addService(service)
                .intercept(interceptor)
                .build();
    }

    public void start() {
        try {
//        Starts the server on the mentioned port
            this.server.start();
            logger.info("Server {} started, listening on port {}.", serverId, Config.getServerPort(serverId));
//        Keeps the server on till terminated
            this.server.awaitTermination();
        } catch (IOException e) {
            logger.error("Server {}: Error in starting GRPC server : {}", serverId, e.getMessage());
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.error("Server {}: GRPC server interrupted : {}", serverId, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        server.shutdown();
    }
}
