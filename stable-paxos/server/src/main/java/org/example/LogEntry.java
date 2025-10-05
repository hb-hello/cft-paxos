package org.example;

public record LogEntry(
        long sequenceNumber,
        int acceptedVotes,
        Status status,
        MessageServiceOuterClass.ClientRequest request
) {
    @Override
    public String toString() {
        return "LogEntry{" +
                "sequenceNumber=" + sequenceNumber +
                ", request=" + request +
                ", status=" + status +
                ", acceptedVotes=" + acceptedVotes +
                '}';
    }
}
