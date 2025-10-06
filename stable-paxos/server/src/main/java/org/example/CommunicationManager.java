package org.example;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class CommunicationManager {

    private static final Logger logger = LogManager.getLogger(CommunicationManager.class);

    private final Server server;
    private final ServerActivityInterceptor interceptor;
    private final Map<String, ServerDetails> servers;
    private final String serverId;

    public CommunicationManager(String serverId, MessageService service) {
        this.servers = Config.getServers();
        this.serverId = serverId;

//        for listening / receiving
        this.interceptor = new ServerActivityInterceptor();
        this.server = ServerBuilder.forPort(Config.getServerPort(serverId))
                .addService(service)
                .intercept(interceptor)
                .build();
    }

    private MessageServiceGrpc.MessageServiceBlockingStub createStub(String targetServerId) {
        ManagedChannel channel = ChannelManager.createOrGetChannel(targetServerId);
        return MessageServiceGrpc.newBlockingStub(channel);
    }

    public MessageServiceOuterClass.PromiseMessage sendPrepare(String targetServerId, Ballot ballot) {
        logger.info("<PREPARE, <{}, {}>> sent to server {}", ballot.getTerm(), ballot.getServerId(), targetServerId);
        MessageServiceOuterClass.Ballot protoBallot = ballot.toProtoBallot();
        MessageServiceOuterClass.PrepareMessage prepareMessage = MessageServiceOuterClass.PrepareMessage.newBuilder().setBallot(protoBallot).build();
        return createStub(targetServerId).prepare(prepareMessage);
    }

    public void start() {
        try {
//        Starts the server on the mentioned port
            this.server.start();
            logger.debug("Server {} started, listening on port {}.", serverId, Config.getServerPort(serverId));
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
