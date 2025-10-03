package org.example;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.example.ConfigLoader.loadServersFromConfig;

public class ServerNode {

    private static final Logger logger = LogManager.getLogger(ServerNode.class);
    private final String SERVER_DETAILS_FILE_PATH = "src/main/resources/serverDetails.json";
    private final String CLIENT_DETAILS_FILE_PATH = "src/main/resources/clientDetails.json";
    private final int MAJORITY_COUNT = 2;
    private final Map<String, ServerDetails> servers;
    private final Server server;
    private final int port;
    private final String serverId;
    private boolean activeStatus = false;
    private final ClientState clientState;

    public void setActiveStatus(boolean activeStatus) {
        this.activeStatus = activeStatus;
    }

    public ServerNode(String serverId) {
        this.serverId = serverId;
        this.clientState = new ClientState(serverId, CLIENT_DETAILS_FILE_PATH);

//        Fetch port from config and create GRPC server
        try {
            this.servers = loadServersFromConfig(SERVER_DETAILS_FILE_PATH);
            if (!servers.containsKey(serverId)) {
                logger.error("Server {} not found in server configuration file {}", serverId, SERVER_DETAILS_FILE_PATH);
                throw new RuntimeException();
            }
            this.port = servers.get(serverId).port();
            this.server = ServerBuilder.forPort(port).addService(new MessageService()).build();
        } catch (Exception e) {
            logger.error("Server {} : Failed to load server details from default config file {} : {}", serverId, SERVER_DETAILS_FILE_PATH, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public boolean getActiveStatus() {
        return activeStatus;
    }

    public String getServerId() {
        return serverId;
    }

    private static class MessageService extends MessageServiceGrpc.MessageServiceImplBase {
        private static final Logger logger = LogManager.getLogger(MessageService.class);

        //        Output of the RPC executed on the server is added to the StreamObserver passed
        @Override
        public void request(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
            responseObserver.onNext(processClientRequest(request));
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