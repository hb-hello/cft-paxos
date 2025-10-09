package org.example;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class LogEntry {
    private final long sequenceNumber;
    private AtomicInteger acceptedVotes;
    private AtomicReference<Status> status;
    private final Ballot ballot;
    private final MessageServiceOuterClass.ClientRequest request;

    public LogEntry(
            long sequenceNumber,
            int acceptedVotes,
            Status status,
            Ballot ballot,
            MessageServiceOuterClass.ClientRequest request
    ) {
        this.sequenceNumber = sequenceNumber;
        this.acceptedVotes = new AtomicInteger(acceptedVotes);
        this.status = new AtomicReference<>(status);
        this.ballot = ballot;
        this.request = request;
    }

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

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public int getAcceptedVotes() {
        return acceptedVotes.get();
    }

    public Status getStatus() {
        return status.get();
    }

    public Ballot getBallot() {
        return ballot;
    }

    public MessageServiceOuterClass.ClientRequest getRequest() {
        return request;
    }

    public int incrementVotes() {
        return acceptedVotes.incrementAndGet();
    }

    public void setStatus(Status newStatus) {
        this.status.set(newStatus);
    }

    public boolean isAccepted() {
        return this.status.get() == Status.ACCEPTED;
    }

    public boolean isCommitted() {
        return this.status.get() == Status.COMMITTED;
    }

    public boolean isExecuted() {
        return this.status.get() == Status.EXECUTED;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LogEntry) obj;
        return this.sequenceNumber == that.sequenceNumber &&
                this.acceptedVotes.get() == that.acceptedVotes.get() &&
                Objects.equals(this.status, that.status) &&
                Objects.equals(this.ballot, that.ballot) &&
                Objects.equals(this.request, that.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequenceNumber, acceptedVotes, status, ballot, request);
    }

}
