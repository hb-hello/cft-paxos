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
        logger.info("MESSAGE: <REQUEST, ({}, {}, {}), {}, {}> received from client {}",
                request.getTransaction().getSender(),
                request.getTransaction().getReceiver(),
                request.getTransaction().getAmount(),
                request.getTimestamp(),
                request.getClientId(),
                request.getClientId()
        );

        // Handle the client request asynchronously
        serverNode.handleClientRequest(request, responseObserver);
    }

    public void setActiveFlag(MessageServiceOuterClass.ActiveFlag request, StreamObserver<MessageServiceOuterClass.Acknowledgement> responseObserver) {
        serverNode.setActive(request.getActiveFlag());
        MessageServiceOuterClass.Acknowledgement ack = MessageServiceOuterClass.Acknowledgement.newBuilder().setStatus(true).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    public void prepare(MessageServiceOuterClass.PrepareMessage request, StreamObserver<MessageServiceOuterClass.PromiseMessage> responseObserver) {
        logger.info("MESSAGE: <PREPARE, <{}, {}>> received from server {}",
                request.getBallot().getInstance(),
                request.getBallot().getSenderId(),
                request.getBallot().getSenderId()
        );
        MessageServiceOuterClass.PromiseMessage promise = serverNode.handlePrepare(request);
        if (promise == null) {
//            do nothing
        } else {
        responseObserver.onNext(promise);
        responseObserver.onCompleted();
        }
    }

}
