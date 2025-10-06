package org.example;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MessageService extends MessageServiceGrpc.MessageServiceImplBase {

    private static final Logger logger = LogManager.getLogger(MessageService.class);
    private final ServerNode serverNode;
    private final ServerState serverState;

    public MessageService(ServerNode serverNode) {
        this.serverNode = serverNode;
        this.serverState = serverNode.getState();
    }

    //        Output of the RPC executed on the server is added to the StreamObserver passed
    @Override
    public void request(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        responseObserver.onNext(processClientRequest(request));
        responseObserver.onCompleted();
    }

    public void setActiveFlag(MessageServiceOuterClass.ActiveFlag request, StreamObserver<MessageServiceOuterClass.Acknowledgement> responseObserver) {
        serverState.setActive(request.getActiveFlag());
        if (request.getActiveFlag()) {
            logger.debug("Server {} activated.", serverNode.getServerId());
        } else {
            logger.debug("Server {} deactivated.", serverNode.getServerId());
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
            if (serverState.isBackup()) {
                //forward the request to leader
            } else {
                serverNode.addToLog(request); // maybe call it addToLogAndReplicate?
            }
        }
        return MessageServiceOuterClass.ClientReply.newBuilder().build();
    }

}
