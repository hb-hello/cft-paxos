package org.example;

import java.util.Objects;

public class Ballot {
    private volatile MessageServiceOuterClass.Ballot protoBallot;
    private final Object lock = new Object();

    public Ballot(int term, String serverId) {
        this.protoBallot = MessageServiceOuterClass.Ballot.newBuilder()
                .setInstance(term)
                .setSenderId(serverId)
                .build();
    }

    public Ballot(Ballot other) {
        synchronized(other.lock) {
            this.protoBallot = other.protoBallot.toBuilder().build();
        }
    }

    public int getTerm() {
        synchronized(lock) {
            return protoBallot.getInstance();
        }
    }

    public void setTerm(int term) {
        synchronized(lock) {
            this.protoBallot = protoBallot.toBuilder()
                    .setInstance(term)
                    .build();
        }
    }

    public String getServerId() {
        synchronized(lock) {
            return protoBallot.getSenderId();
        }
    }

    public void setServerId(String serverId) {
        synchronized(lock) {
            this.protoBallot = protoBallot.toBuilder()
                    .setSenderId(serverId)
                    .build();
        }
    }

    public void setBallot(int term, String serverId) {
        synchronized(lock) {
            this.protoBallot = protoBallot.toBuilder()
                    .setInstance(term)
                    .setSenderId(serverId)
                    .build();
        }
    }

    public void increment() {
        synchronized(lock) {
            int currentTerm = protoBallot.getInstance();
            this.protoBallot = protoBallot.toBuilder()
                    .setInstance(currentTerm + 1)
                    .build();
        }
    }

    public boolean isGreaterThan(Ballot other) {
        int thisTerm, otherTerm;
        String thisServerId, otherServerId;

        synchronized(this.lock) {
            thisTerm = this.protoBallot.getInstance();
            thisServerId = this.protoBallot.getSenderId();
        }

        synchronized(other.lock) {
            otherTerm = other.protoBallot.getInstance();
            otherServerId = other.protoBallot.getSenderId();
        }

        if (thisTerm != otherTerm) {
            return thisTerm > otherTerm;
        }
        return thisServerId.compareTo(otherServerId) > 0;
    }

    public MessageServiceOuterClass.Ballot toProtoBallot() {
        synchronized(lock) {
            return protoBallot.toBuilder().build();
        }
    }

    public static Ballot fromProtoBallot(MessageServiceOuterClass.Ballot protoBallot) {
        return new Ballot(protoBallot.getInstance(), protoBallot.getSenderId());
    }
}
