package org.example;

public class Ballot {
    private MessageServiceOuterClass.Ballot protoBallot;

    public Ballot(int term, String serverId) {
        this.protoBallot = MessageServiceOuterClass.Ballot.newBuilder()
                .setInstance(term)
                .setSenderId(serverId)
                .build();
    }

    public Ballot(Ballot other) {
        this.protoBallot = other.protoBallot.toBuilder().build();
    }

    public int getTerm() {
        return protoBallot.getInstance();
    }

    public void setTerm(int term) {
        this.protoBallot = protoBallot.toBuilder()
                .setInstance(term)
                .build();
    }

    public String getServerId() {
        return protoBallot.getSenderId();
    }

    public void setServerId(String serverId) {
        this.protoBallot = protoBallot.toBuilder()
                .setSenderId(serverId)
                .build();
    }

    public void setBallot(int term, String serverId) {
        setTerm(term);
        setServerId(serverId);
    } // might not need this function

    public void increment() {
        setTerm(getTerm() + 1);
    }

    public boolean isGreaterThan(Ballot other) {
        if (this.getTerm() != other.getTerm()) {
            return this.getTerm() > other.getTerm();
        }
        return this.getServerId().compareTo(other.getServerId()) > 0;
    }

    public MessageServiceOuterClass.Ballot toProtoBallot() {
        return protoBallot;
    }

    public static Ballot fromProtoBallot(MessageServiceOuterClass.Ballot protoBallot) {
        Ballot ballot = new Ballot(protoBallot.getInstance(), protoBallot.getSenderId());
        return ballot;
    }
}
