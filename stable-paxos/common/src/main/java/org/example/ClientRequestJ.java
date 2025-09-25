package org.example;

public class ClientRequestJ {
    private final String transaction;
    private final long timestamp;
    private final String clientId;

    public ClientRequestJ(String transaction, long timestamp, String clientId) {
        this.transaction = transaction;
        this.timestamp = timestamp;
        this.clientId = clientId;
    }

    // Getters and other methods as needed
}