package org.example;

public record LogEntry(
        long sequenceNumber,
        int acceptedVotes,
        Status status,
        MessageServiceOuterClass.Transaction transaction
) {
}
