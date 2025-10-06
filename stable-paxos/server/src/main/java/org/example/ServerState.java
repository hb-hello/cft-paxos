package org.example;

import io.netty.util.internal.shaded.org.jctools.queues.atomic.AtomicQueueUtil;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ServerState {
    private AtomicReference<Role> role;
    private final AtomicReference<String> leaderId;
    private final Ballot ballot;
    private final LinkedBlockingQueue<MessageServiceOuterClass.PromiseMessage> promiseMessages;

    public ServerState(String serverId) {
        this.ballot = new Ballot(1, serverId);
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

    public Ballot getBallot() {
        return ballot;
    }

    public int addPromise(MessageServiceOuterClass.PromiseMessage promise) {
        promiseMessages.add(promise);

        return promiseMessages.size(); //not keeping it synchronized as size will only increase
    }

    public void clearPromises() {
        promiseMessages.clear();
    }
}
