package org.example;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

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

    private MessageServiceGrpc.MessageServiceBlockingStub createBlockingStub(String targetServerId) {
        ManagedChannel channel = ChannelManager.createOrGetChannel(targetServerId);
        return MessageServiceGrpc.newBlockingStub(channel);
    }

    private MessageServiceGrpc.MessageServiceStub createStub(String targetServerId) {
        ManagedChannel channel = ChannelManager.createOrGetChannel(targetServerId);
        return MessageServiceGrpc.newStub(channel);
    }

    public MessageServiceOuterClass.PromiseMessage sendPrepare(String targetServerId, Ballot ballot) {
        logger.info("MESSAGE: <PREPARE, <{}, {}>> sent to server {}", ballot.getTerm(), ballot.getServerId(), targetServerId);
        MessageServiceOuterClass.Ballot protoBallot = ballot.toProtoBallot();
        MessageServiceOuterClass.PrepareMessage prepareMessage = MessageServiceOuterClass.PrepareMessage.newBuilder().setBallot(protoBallot).build();
        return createBlockingStub(targetServerId).prepare(prepareMessage);
    }

    public void sendNewView(String targetServerId, MessageServiceOuterClass.NewViewMessage newViewMessage, AcceptedHandler handler) {
        logger.info("MESSAGE: <NEW VIEW, <{}, {}>, acceptLog({} messages)> sent to server {}",
                newViewMessage.getBallot().getInstance(),
                newViewMessage.getBallot().getSenderId(),
                newViewMessage.getAcceptLogCount(),
                targetServerId);
        createStub(targetServerId).newView(newViewMessage, new StreamObserver<MessageServiceOuterClass.AcceptedMessage>() {

        @Override
        public void onNext(MessageServiceOuterClass.AcceptedMessage acceptedMessage) {
            handler.handle(acceptedMessage);
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("Error while receiving accepted message for new view sent earlier");
        }

        @Override
        public void onCompleted() {
            logger.info("Completed receiving accepted messages for new view sent earlier");
        }
    });
    }

    public MessageServiceOuterClass.AcceptedMessage sendAccept(String targetServerId, MessageServiceOuterClass.AcceptMessage acceptMessage) {
        logger.info("MESSAGE: <ACCEPT, <{}, {}>, {}, ({}, {}, {})> sent to server {}",
                acceptMessage.getBallot().getInstance(),
                acceptMessage.getBallot().getSenderId(),
                acceptMessage.getSequenceNumber(),
                acceptMessage.getRequest().getTransaction().getSender(),
                acceptMessage.getRequest().getTransaction().getReceiver(),
                acceptMessage.getRequest().getTransaction().getAmount(),
                targetServerId);
        return createBlockingStub(targetServerId).accept(acceptMessage);
    }

    public void forwardClientRequest(String leaderId, MessageServiceOuterClass.ClientRequest request) {
        logger.info("Forwarding client request to leader {}.", leaderId);
    }

    public void sendCommit(String targetServerId, MessageServiceOuterClass.CommitMessage commitMessage) {
        logger.info("MESSAGE: <COMMIT, <{}, {}>, {}, ({}, {}, {})> sent to server {}",
                commitMessage.getBallot().getInstance(),
                commitMessage.getBallot().getSenderId(),
                commitMessage.getSequenceNumber(),
                commitMessage.getRequest().getTransaction().getSender(),
                commitMessage.getRequest().getTransaction().getReceiver(),
                commitMessage.getRequest().getTransaction().getAmount(),
                targetServerId);
        createBlockingStub(targetServerId).commit(commitMessage);
    }

    public void startListening() {
        try {
//        Starts the server on the mentioned port
            this.server.start();
            logger.info("GRPC server for node {} started, listening on port {}.", serverId, Config.getServerPort(serverId));
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
