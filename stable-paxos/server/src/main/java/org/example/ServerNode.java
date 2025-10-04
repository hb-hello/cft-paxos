package org.example;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class ServerNode {

    private static final Logger logger = LogManager.getLogger(ServerNode.class);
    private final int MAJORITY_COUNT = 2;
    private final Map<String, ServerDetails> servers;
    private final Server server;
    private final int port;
    private final String serverId;
    private boolean activeStatus = false;
    private final ClientState clientState;
//    private final Log log;

    public ServerNode(String serverId) {
        this.serverId = serverId;
        this.clientState = new ClientState(serverId);
        this.servers = Config.getServers();

//        Fetch port from config and create GRPC server
        this.port = Config.getServerPort(serverId);
        this.server = ServerBuilder.forPort(port).addService(new MessageService(this)).build();
    }

    public boolean getActiveStatus() {
        return activeStatus;
    }

    public void setActiveStatus(boolean activeStatus) {
        this.activeStatus = activeStatus;
    }

    public String getServerId() {
        return serverId;
    }

    private static class MessageService extends MessageServiceGrpc.MessageServiceImplBase {
        private static final Logger logger = LogManager.getLogger(MessageService.class);
        private final ServerNode serverNode;

        public MessageService(ServerNode serverNode) {
            this.serverNode = serverNode;
        }

        //        Output of the RPC executed on the server is added to the StreamObserver passed
        @Override
        public void request(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
            if (serverNode.getActiveStatus()) {
                responseObserver.onNext(processClientRequest(request));
            }
            responseObserver.onCompleted();
        }

        public void setActiveFlag(MessageServiceOuterClass.ActiveFlag request, StreamObserver<MessageServiceOuterClass.Acknowledgement> responseObserver) {
            serverNode.setActiveStatus(request.getActiveFlag());
            if (request.getActiveFlag()) {
                logger.info("Server activated.");
            } else {
                logger.info("Server deactivated.");
            }
            MessageServiceOuterClass.Acknowledgement ack = MessageServiceOuterClass.Acknowledgement.newBuilder().setStatus(true).build();
            responseObserver.onNext(ack);
            responseObserver.onCompleted();
        }

        private MessageServiceOuterClass.ClientReply processClientRequest(MessageServiceOuterClass.ClientRequest request) {
            System.out.println("received something");
            return MessageServiceOuterClass.ClientReply.newBuilder().build();
        }

    }

    public void start() {
        try {
//        Starts the server on the mentioned port
            this.server.start();
            logger.info("Server {} started, listening on port {}.", serverId, port);
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

    public static void main(String[] args) {
        Config.initialize();

        if (args.length == 0) {
            logger.error("No server ID provided while initializing server.");
            return;
        }
//        First argument will be server ID
        String serverId = args[0];
        ServerNode serverNode = new ServerNode(serverId);
        serverNode.start();
    }
}