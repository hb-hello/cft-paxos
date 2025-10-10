package org.example;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MessageService extends MessageServiceGrpc.MessageServiceImplBase {

    private static final Logger logger = LogManager.getLogger(MessageService.class);
    private final ServerNode serverNode;

    public MessageService(ServerNode serverNode) {
        this.serverNode = serverNode;
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
        MessageServiceOuterClass.PromiseMessage promise = serverNode.handlePrepare(request, responseObserver);
        if (promise == null) {
//            do nothing
        } else {
            responseObserver.onNext(promise);
            responseObserver.onCompleted();
        }
    }

    public void newView(MessageServiceOuterClass.NewViewMessage request, StreamObserver<MessageServiceOuterClass.AcceptedMessage> responseObserver) {
        logger.info("MESSAGE: <NEW VIEW, <{}, {}>, acceptLog({} messages)> received from server {}",
                request.getBallot().getInstance(),
                request.getBallot().getSenderId(),
                request.getAcceptLogCount(),
                request.getBallot().getSenderId());
        serverNode.handleNewView(request, responseObserver);
    }

    public void accept(MessageServiceOuterClass.AcceptMessage request, StreamObserver<MessageServiceOuterClass.AcceptedMessage> responseObserver) {
        logger.info("MESSAGE: <ACCEPT, <{}, {}>, {}, ({}, {}, {})> received from server {}",
                request.getBallot().getInstance(),
                request.getBallot().getSenderId(),
                request.getSequenceNumber(),
                request.getRequest().getTransaction().getSender(),
                request.getRequest().getTransaction().getReceiver(),
                request.getRequest().getTransaction().getAmount(),
                request.getBallot().getSenderId());
        responseObserver.onNext(serverNode.handleAccept(request));
        responseObserver.onCompleted();
    }

    public void commit(MessageServiceOuterClass.CommitMessage request, StreamObserver<Empty> responseObserver) {
        logger.info("<COMMIT <{}, {}>, {}, ({}, {}, {})> received from server {}",
                request.getBallot().getInstance(),
                request.getBallot().getSenderId(),
                request.getSequenceNumber(),
                request.getRequest().getTransaction().getSender(),
                request.getRequest().getTransaction().getReceiver(),
                request.getRequest().getTransaction().getAmount(),
                request.getBallot().getSenderId());

        serverNode.handleCommitMessage(request);

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    public void forwardClientRequest(MessageServiceOuterClass.ClientRequest request, StreamObserver<Empty> responseObserver) {
        logger.info("MESSAGE: <REQUEST, ({}, {}, {}), {}, {}> forwarded from another server",
                request.getTransaction().getSender(),
                request.getTransaction().getReceiver(),
                request.getTransaction().getAmount(),
                request.getTimestamp(),
                request.getClientId()
        );

        serverNode.handleClientRequest(request, null);

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
