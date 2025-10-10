package org.example;

import java.util.Objects;

public class Ballot {
    private volatile MessageServiceOuterClass.Ballot protoBallot;
    private final Object lock = new Object();

    public Ballot(int term, String serverId) {
        this.protoBallot = MessageServiceOuterClass.Ballot.newBuilder().setInstance(term).setSenderId(serverId).build();
    }

    public Ballot(Ballot other) {
        synchronized (other.lock) {
            this.protoBallot = other.protoBallot.toBuilder().build();
        }
    }

    public int getTerm() {
        synchronized (lock) {
            return protoBallot.getInstance();
        }
    }

    public void setTerm(int term) {
        synchronized (lock) {
            this.protoBallot = protoBallot.toBuilder().setInstance(term).build();
        }
    }

    public String getServerId() {
        synchronized (lock) {
            return protoBallot.getSenderId();
        }
    }

    public void setServerId(String serverId) {
        synchronized (lock) {
            this.protoBallot = protoBallot.toBuilder().setSenderId(serverId).build();
        }
    }

    public void setBallot(int term, String serverId) {
        synchronized (lock) {
            System.out.println("Setting ballot to " + term + ", " + serverId);
            this.protoBallot = protoBallot.toBuilder().setInstance(term).setSenderId(serverId).build();
        }
    }

    public void increment(String byServerId) {
        synchronized (lock) {
            int currentTerm = protoBallot.getInstance();
            int newTerm = currentTerm + 1;
            System.out.println("Incrementing ballot to " + newTerm + ", " + byServerId);
            this.protoBallot = protoBallot.toBuilder().setInstance(newTerm).setSenderId(byServerId).build();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Ballot ballot = (Ballot) o;
        return this.getTerm() == ballot.getTerm() && Objects.equals(this.getServerId(), ballot.getServerId());
    }

    public boolean isGreaterThan(Ballot other) {
        int thisTerm, otherTerm;
        String thisServerId, otherServerId;

        synchronized (this.lock) {
            thisTerm = this.protoBallot.getInstance();
            thisServerId = this.protoBallot.getSenderId();
        }

        synchronized (other.lock) {
            otherTerm = other.protoBallot.getInstance();
            otherServerId = other.protoBallot.getSenderId();
        }

        if (thisTerm != otherTerm) {
            return thisTerm > otherTerm;
        }
        boolean compare = thisServerId.compareTo(otherServerId) > 0;
        if (compare) {
            System.out.println(thisServerId + " is greater than " + otherServerId);
        } else System.out.println(thisServerId + " is not greater than " + otherServerId);
        return thisServerId.compareTo(otherServerId) > 0;
    }

    public boolean isGreaterThanOrEqual(Ballot other) {
        return this.equals(other) || this.isGreaterThan(other);
    }

    public MessageServiceOuterClass.Ballot toProtoBallot() {
        synchronized (lock) {
            return protoBallot.toBuilder().build();
        }
    }

    public static Ballot fromProtoBallot(MessageServiceOuterClass.Ballot protoBallot) {
        return new Ballot(protoBallot.getInstance(), protoBallot.getSenderId());
    }

    @Override
    public String toString() {
        return '<' + protoBallot.getInstance() + ", " + protoBallot.getSenderId() + '>';
    }
}
