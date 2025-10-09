package org.example;

import io.grpc.stub.StreamObserver;

public record PendingRequest(MessageServiceOuterClass.ClientRequest request,
                             StreamObserver<MessageServiceOuterClass.ClientReply> responseObserver) {
    public PendingRequest(MessageServiceOuterClass.ClientRequest request) {
        this(request, null);
    }
}
