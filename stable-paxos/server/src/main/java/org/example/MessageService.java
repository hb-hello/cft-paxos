package org.example;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.example.CLILogging.formatNewViews;
import static org.example.CLILogging.mapStatus;

public class MessageService extends MessageServiceGrpc.MessageServiceImplBase {

    private static final Logger logger = LogManager.getLogger(MessageService.class);
    private final ServerNode serverNode;

    public MessageService(ServerNode serverNode) {
        this.serverNode = serverNode;
    }

    //        Output of the RPC executed on the server is added to the StreamObserver passed

    @Override
    public void request(MessageServiceOuterClass.ClientRequest request, StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        serverNode.logCommunication(
                String.format("MESSAGE: <REQUEST, (%s, %s, %f), %d, %s> received from client %s",
                        request.getTransaction().getSender(),
                        request.getTransaction().getReceiver(),
                        request.getTransaction().getAmount(),
                        request.getTimestamp(),
                        request.getClientId(),
                        request.getClientId()
                )
        );
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

    @Override
    public void setActiveFlag(MessageServiceOuterClass.ActiveFlag request, StreamObserver<MessageServiceOuterClass.Acknowledgement> responseObserver) {
        serverNode.setActive(request.getActiveFlag());
        MessageServiceOuterClass.Acknowledgement ack = MessageServiceOuterClass.Acknowledgement.newBuilder().setStatus(true).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void prepare(MessageServiceOuterClass.PrepareMessage request, StreamObserver<MessageServiceOuterClass.PromiseMessage> responseObserver) {
        serverNode.logCommunication(
                String.format("MESSAGE: <PREPARE, <%d, %s>> received from server %s",
                        request.getBallot().getInstance(),
                        request.getBallot().getSenderId(),
                        request.getBallot().getSenderId()
                )
        );
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

    @Override
    public void newView(MessageServiceOuterClass.NewViewMessage request, StreamObserver<MessageServiceOuterClass.AcceptedMessage> responseObserver) {
        serverNode.logCommunication(
                String.format("MESSAGE: <NEW VIEW, <%d, %s>, acceptLog(%d messages)> received from server %s",
                        request.getBallot().getInstance(),
                        request.getBallot().getSenderId(),
                        request.getAcceptLogCount(),
                        request.getBallot().getSenderId())
        );
        logger.info("MESSAGE: <NEW VIEW, <{}, {}>, acceptLog({} messages)> received from server {}",
                request.getBallot().getInstance(),
                request.getBallot().getSenderId(),
                request.getAcceptLogCount(),
                request.getBallot().getSenderId());
        serverNode.handleNewView(request, responseObserver);
    }

    @Override
    public void accept(MessageServiceOuterClass.AcceptMessage request, StreamObserver<MessageServiceOuterClass.AcceptedMessage> responseObserver) {
        serverNode.logCommunication(
                String.format("MESSAGE: <ACCEPT, <%d, %s>, %d, (%s, %s, %f)> received from server %s",
                        request.getBallot().getInstance(),
                        request.getBallot().getSenderId(),
                        request.getSequenceNumber(),
                        request.getRequest().getTransaction().getSender(),
                        request.getRequest().getTransaction().getReceiver(),
                        request.getRequest().getTransaction().getAmount(),
                        request.getBallot().getSenderId())
        );
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

    @Override
    public void commit(MessageServiceOuterClass.CommitMessage request, StreamObserver<Empty> responseObserver) {
        serverNode.logCommunication(
                String.format("MESSAGE: <COMMIT, <%d, %s>, %d, (%s, %s, %f)> received from server %s",
                        request.getBallot().getInstance(),
                        request.getBallot().getSenderId(),
                        request.getSequenceNumber(),
                        request.getRequest().getTransaction().getSender(),
                        request.getRequest().getTransaction().getReceiver(),
                        request.getRequest().getTransaction().getAmount(),
                        request.getBallot().getSenderId())
        );
        logger.info("MESSAGE: <COMMIT <{}, {}>, {}, ({}, {}, {})> received from server {}",
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

    @Override
    public void forwardClientRequest(MessageServiceOuterClass.ClientRequest request, StreamObserver<Empty> responseObserver) {
        serverNode.logCommunication(
                String.format("MESSAGE: <REQUEST, (%s, %s, %f), %d, %s> forwarded from another server",
                        request.getTransaction().getSender(),
                        request.getTransaction().getReceiver(),
                        request.getTransaction().getAmount(),
                        request.getTimestamp(),
                        request.getClientId()
                )
        );
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

    @Override
    public void failLeader(Empty request, StreamObserver<MessageServiceOuterClass.Acknowledgement> responseObserver) {
        logger.info("<FAIL LEADER> received from cli");
        MessageServiceOuterClass.Acknowledgement ack;
        if (serverNode.getState().isLeader()) {
            serverNode.setActive(false);
            logger.info("Node {} (leader) has been deactivated to simulate leader failure.", serverNode.getServerId());
            ack = MessageServiceOuterClass.Acknowledgement.newBuilder().setStatus(true).build();
        } else {
            logger.info("Node {} is not the leader. Ignoring FAIL LEADER command.", serverNode.getServerId());
            ack = MessageServiceOuterClass.Acknowledgement.newBuilder().setStatus(false).build();
        }
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void getLog(Empty request, StreamObserver<MessageServiceOuterClass.CLIResponse> responseObserver) {
        String logString = CLILogging.formatLog(serverNode.getCommunicationLog());
        MessageServiceOuterClass.CLIResponse response = MessageServiceOuterClass.CLIResponse.newBuilder().setCliResponse(logString).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getDB(Empty request, StreamObserver<MessageServiceOuterClass.CLIResponse> responseObserver) {
        String stateString = CLILogging.formatState(serverNode.getClientState());
        MessageServiceOuterClass.CLIResponse response = MessageServiceOuterClass.CLIResponse.newBuilder().setCliResponse(stateString).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getStatus(MessageServiceOuterClass.SequenceNumber request, StreamObserver<MessageServiceOuterClass.CLIResponse> responseObserver) {
        String statusString = mapStatus(serverNode.getLog().getStatus(request.getSequenceNumber()));
        MessageServiceOuterClass.CLIResponse response = MessageServiceOuterClass.CLIResponse.newBuilder().setCliResponse(statusString).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getNewViews(Empty request, StreamObserver<MessageServiceOuterClass.CLIResponse> responseObserver) {
        String newViewsString = formatNewViews(serverNode.getNewViews());
        MessageServiceOuterClass.CLIResponse response = MessageServiceOuterClass.CLIResponse.newBuilder().setCliResponse(newViewsString).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
