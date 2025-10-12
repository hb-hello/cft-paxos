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
            return protoBallot.getInstance();
    }

    public void setTerm(int term) {
        synchronized (lock) {
            this.protoBallot = protoBallot.toBuilder().setInstance(term).build();
        }
    }

    public String getServerId() {
            return protoBallot.getSenderId();
    }

    public void setServerId(String serverId) {
        synchronized (lock) {
            this.protoBallot = protoBallot.toBuilder().setSenderId(serverId).build();
        }
    }

    public void setBallot(int term, String serverId) {
        synchronized (lock) {
            System.out.println("Setting ballot to " + term + ", " + serverId);
            this.protoBallot = MessageServiceOuterClass.Ballot.newBuilder().setInstance(term).setSenderId(serverId).build();
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
        Object lock1 = System.identityHashCode(this) < System.identityHashCode(other) ? this.lock : other.lock;
        Object lock2 = System.identityHashCode(this) < System.identityHashCode(other) ? other.lock : this.lock;

        synchronized (lock1) {
            synchronized (lock2) {
                // Read volatile fields after acquiring locks
                MessageServiceOuterClass.Ballot thisProto = this.protoBallot;
                MessageServiceOuterClass.Ballot otherProto = other.protoBallot;

                if (thisProto.getInstance() != otherProto.getInstance()) {
                    return thisProto.getInstance() > otherProto.getInstance();
                }
                return thisProto.getSenderId().compareTo(otherProto.getSenderId()) > 0;
            }
        }
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
        return "<" + protoBallot.getInstance() + ", " + protoBallot.getSenderId() + ">";
    }
}
