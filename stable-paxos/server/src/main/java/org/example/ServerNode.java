package org.example;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ServerNode {

    private static final Logger logger = LogManager.getLogger(ServerNode.class);

    private final ServerActivityInterceptor interceptor;
    private final Server server;

    private final int MAJORITY_COUNT = 2;
    private final Map<String, ServerDetails> servers;
    private final int port;
    private final String serverId;
    private boolean active;
    private final ClientState clientState;
    private final Log log;
    private Role role;
    private String leaderId;
    private MessageServiceOuterClass.Ballot ballot; // make a separate class? 3 methods are needed - increment, update and isGreaterThan
    private final List<MessageServiceOuterClass.Promise> promisesReceived;
    private boolean waitBeforePrepare;

    public ServerNode(String serverId) {
        this.serverId = serverId;
        this.clientState = new ClientState(serverId);
        this.servers = Config.getServers();
        this.log = new Log(serverId);
        this.interceptor = new ServerActivityInterceptor();
        this.active = false;
        this.leaderId = null; // initialize leader from persisted server state?
        this.promisesReceived = new ArrayList<>();

//        Fetch port from config and create GRPC server
        this.port = Config.getServerPort(serverId);
        this.server = ServerBuilder.forPort(port).addService(new MessageService(this)).intercept(interceptor) // Interceptor to stop incoming requests when inactive
                .build();

        // initializeRole -> if leader id is null then becomeCandidate
    }

    public boolean getActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active; // Governs sending of RPCs
        this.interceptor.setActiveFlag(active);
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
            responseObserver.onNext(processClientRequest(request));
            responseObserver.onCompleted();
        }

        public void setActiveFlag(MessageServiceOuterClass.ActiveFlag request, StreamObserver<MessageServiceOuterClass.Acknowledgement> responseObserver) {
            serverNode.setActive(request.getActiveFlag());
            if (request.getActiveFlag()) {
                logger.info("Server {} activated.", serverNode.getServerId());
            } else {
                logger.info("Server {} deactivated.", serverNode.getServerId());
            }
            MessageServiceOuterClass.Acknowledgement ack = MessageServiceOuterClass.Acknowledgement.newBuilder().setStatus(true).build();
            responseObserver.onNext(ack);
            responseObserver.onCompleted();
        }

        private MessageServiceOuterClass.ClientReply processClientRequest(MessageServiceOuterClass.ClientRequest request) {
            System.out.println("received something");
            if (serverNode.compareTimestampAgainstLog(request)) {
                // don't add anything to log
                // check if reply is cached in replyCache and send that in response
            } else {
                if (serverNode.isBackup()) {
                    //forward the request to leader
                } else {
                    serverNode.addToLog(request); // maybe call it addToLogAndReplicate?
                }
            }
            return MessageServiceOuterClass.ClientReply.newBuilder().build();
        }

    }

    public boolean isCandidate() {
        return role == Role.CANDIDATE;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public boolean isBackup() {
        return role == Role.BACKUP;
    }

    // triggered whenever backup role's timer expires or when leader id is null (on startup)
    public void becomeCandidate() {
        this.role = Role.CANDIDATE;

//        start timer waitBeforePrepare
    }

    //    triggered when promiseQueue collects a majority quorum of promises
    public void becomeLeader() {
        this.role = Role.LEADER;

//        start heartbeat thread
//        coalesce accept log and send new view
    }

    //    triggered when a prepare/accept/commit/heartbeat(? -> heartbeat should be treated same as empty accept?) with a higher ballot is received
    public void becomeBackup() {
        this.role = Role.BACKUP;

//        close heartbeat thread if it exists
//        start timer leaderLivenessCheck
    }

    public void addToLog(MessageServiceOuterClass.ClientRequest request) {

//        request would not be in log as we're checking for that before calling addToLog
        log.add(request);

        if (role == Role.LEADER) {
//            send accept message
        } else {
//            role would be candidate as backup would never call addToLog
            if (!waitBeforePrepare) {
//            send prepare messages
            }
        }
    }

    //    to check if the client request is already in the log
    private boolean compareTimestampAgainstLog(MessageServiceOuterClass.ClientRequest request) {
        return clientState.getTimestamp(request.getClientId()) < request.getTimestamp();
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