package org.example;

public record LogEntry(
        long sequenceNumber,
        int acceptedVotes,
        Status status,
        Ballot ballot,
        MessageServiceOuterClass.ClientRequest request
) {
    @Override
    public String toString() {
        return "LogEntry{" +
                "sequenceNumber=" + sequenceNumber +
                ", request=" + request +
                ", status=" + status +
                ", ballot=" + ballot +
                ", acceptedVotes=" + acceptedVotes +
                '}';
    }
}
