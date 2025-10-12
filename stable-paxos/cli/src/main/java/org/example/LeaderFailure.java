package org.example;

public final class LeaderFailure implements TransactionEvent {
    @Override
    public String getEventType() {
        return "LEADER_FAILURE";
    }

    @Override
    public String toString() {
        return "LeaderFailure()";
    }
}
