package org.example;

import io.grpc.stub.StreamObserver;

public record LoggedPrepareMessage(MessageServiceOuterClass.PrepareMessage prepareMessage, StreamObserver<MessageServiceOuterClass.PromiseMessage> responseObserver) {
}
