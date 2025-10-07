package org.example;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class ServerState {
    private final String serverId;
    private AtomicReference<Role> role;
    private final AtomicReference<String> leaderId;
    private final Ballot ballot;
    private final LinkedBlockingQueue<MessageServiceOuterClass.PromiseMessage> promiseMessages;

    public ServerState(String serverId) {
        this.serverId = serverId;
        this.ballot = new Ballot(0, serverId);
        this.leaderId = new AtomicReference<>(null);
        this.promiseMessages = new LinkedBlockingQueue<>(10);
//        always initialize as candidate
        this.role = new AtomicReference<>(null);
    }

    public void setRole(Role role) {
        this.role.set(role);
    }

    public Role getRole() {
        return role.get();
    }

    public boolean isCandidate() {
        return role.get() == Role.CANDIDATE;
    }

    public boolean isLeader() {
        return role.get() == Role.LEADER;
    }

    public boolean isBackup() {
        return role.get() == Role.BACKUP;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId.set(leaderId);
    }

    public String getLeaderId() {
        return leaderId.get();
    }

    public Ballot getBallot() {
        return ballot;
    }

    public void setBallot(Ballot ballot) {
        ballot.setBallot(ballot.getTerm(), ballot.getServerId());
    }

    public void incrementBallot() {
        ballot.increment(serverId);
    }

    public int addPromise(MessageServiceOuterClass.PromiseMessage promise) {
        promiseMessages.add(promise);
        return promiseMessages.size(); //not keeping it synchronized as size will only increase
    }

    public void clearPromises() {
        promiseMessages.clear();
    }
}
