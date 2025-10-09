package org.example;

@FunctionalInterface
public interface AcceptedHandler {
    void handle(MessageServiceOuterClass.AcceptedMessage acceptedMessage);
}
