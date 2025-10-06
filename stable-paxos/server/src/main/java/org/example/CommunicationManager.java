package org.example;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class CommunicationManager {

    private static final Logger logger = LogManager.getLogger(CommunicationManager.class);

    private final Server server;
    private final ServerActivityInterceptor interceptor;
    private final String serverId;
    private final Set<String> otherServerIds;
    private boolean active;

    public CommunicationManager(String serverId, MessageService service) {
        this.serverId = serverId;
        this.otherServerIds = Config.getServerIdsExcept(serverId);

//        for listening / receiving
        this.interceptor = new ServerActivityInterceptor();
        this.server = ServerBuilder.forPort(Config.getServerPort(serverId))
                .addService(service)
                .intercept(interceptor)
                .build();
    }

    public void setActive(boolean active) {
        this.active = active;
        this.interceptor.setActiveFlag(active);
        if (active) logger.info("Server activated.");
        else logger.info("Server deactivated.");
    }

    public boolean isActive() {
        return active;
    }

    private MessageServiceGrpc.MessageServiceBlockingStub createStub(String targetServerId) {
        ManagedChannel channel = ChannelManager.createOrGetChannel(targetServerId);
        return MessageServiceGrpc.newBlockingStub(channel);
    }

    public MessageServiceOuterClass.PromiseMessage sendPrepare(String targetServerId, Ballot ballot) {
        logger.info("MESSAGE: <PREPARE, <{}, {}>> sent to server {}", ballot.getTerm(), ballot.getServerId(), targetServerId);
        MessageServiceOuterClass.Ballot protoBallot = ballot.toProtoBallot();
        MessageServiceOuterClass.PrepareMessage prepareMessage = MessageServiceOuterClass.PrepareMessage.newBuilder().setBallot(protoBallot).build();
        return createStub(targetServerId).prepare(prepareMessage);
    }

    public void startListening() {
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
            logger.info("GRPC server was shut down.");
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        server.shutdown();
    }
}
