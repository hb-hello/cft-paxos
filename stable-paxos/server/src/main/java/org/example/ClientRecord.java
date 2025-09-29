package org.example;

public record ClientRecord(String clientId, double bankBalance, long lastTimestampRepliedTo) {
    public ClientRecord(String clientId, double bankBalance) {
        this(clientId, bankBalance, 0L);
    }
}
