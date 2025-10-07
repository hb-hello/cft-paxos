package org.example;

import io.grpc.stub.StreamObserver;

public class PendingRequest {
    final MessageServiceOuterClass.ClientRequest request;
    final StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver;

    PendingRequest(MessageServiceOuterClass.ClientRequest request,
                   StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }
}
