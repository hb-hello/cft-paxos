package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Log {
    private static final Logger logger = LogManager.getLogger(Log.class);
    private final ExecutorService saveExecutor;

    private final ConcurrentHashMap<Long, LogEntry> log; // thread-safe log
    private final ConcurrentHashMap<String, Long> requestIds;
    private final long firstSequenceNumber; // maintains last stable checkpoint
    private final AtomicLong sequenceNumber; // thread-safe sequence number
    private final AtomicLong nextUnexecutedSequence;
    private final String serverId;

    public Log(String serverId) {

        this.serverId = serverId;
        this.saveExecutor = Executors.newSingleThreadExecutor();

        try {
            this.log = new ConcurrentHashMap<>();
            this.requestIds = new ConcurrentHashMap<>();
            this.firstSequenceNumber = 0;
            this.sequenceNumber = new AtomicLong(0);
            this.nextUnexecutedSequence = new AtomicLong(1);
        } catch (Exception e) {
            logger.error("Server {} : Error initializing log", serverId);
            throw new RuntimeException(e);
        }
    }

    //    Gets next entry that should be executed and used to maintain sequential execution order
    public long getNextUnexecutedSequence() {
        return nextUnexecutedSequence.get();
    }

    private void save() {
        try {
            long currentSeq = sequenceNumber.get();
//            long lastSaved = lastSavedSequenceNumber.get();
//
//            if (currentSeq > lastSaved) {
            Map<Long, LogEntry> snapshot = new HashMap<>(log);
            LogLoader.saveLogEntries(serverId, snapshot);
//                lastSavedSequenceNumber.set(currentSeq);
            logger.info("Saved log up to sequence: {}", currentSeq);
//            }
        } catch (Exception e) {
            logger.error("Error during log save: {}", e.getMessage());
        }
    }

    public static String getRequestKey(MessageServiceOuterClass.ClientRequest request) {
        return request.getClientId() + "-" + request.getTimestamp();
    }

//    private Map<Long, LogEntry> load() {
//        try {
//            return LogLoader.loadLogEntries(serverId);
//        } catch (Exception e) {
//            logger.error("Server {} : Error when loading log from file", serverId);
//            throw new RuntimeException(e);
//        }
//    }

    public long add(MessageServiceOuterClass.ClientRequest request, Ballot ballot) {
        logger.info("Adding transaction {} to log.", request.getTransaction());
        String requestKey = getRequestKey(request);
        if (requestIds.containsKey(requestKey)) {
            return requestIds.get(requestKey);
        } else {
            long seqNum = sequenceNumber.incrementAndGet();
            LogEntry logEntry = new LogEntry(seqNum, 0, Status.ACCEPTED, ballot, request);
            log.put(seqNum, logEntry);
            requestIds.put(requestKey, seqNum);

//        trigger async save
            this.saveExecutor.submit(this::save);
            return seqNum;
        }
    }

    public int incrementVotes(long seqNum) {
        logger.info("Incrementing votes for sequence number {}", seqNum);
        LogEntry logEntry = log.get(seqNum);
        if (logEntry == null) return 0;
        else return logEntry.incrementVotes();
    }

    public void updateToCommitted(long seqNum) {
        logger.info("Updating status to committed for sequence number {}", seqNum);
        LogEntry logEntry = log.get(seqNum);
        if (logEntry != null) {
            logEntry.setStatus(Status.COMMITTED);
            this.saveExecutor.submit(this::save);
        }
    }

    public boolean updateToExecuted(long seqNum) {
        LogEntry entry = log.get(seqNum);
        if (entry == null) {
            logger.warn("Cannot mark as executed: entry {} does not exist", seqNum);
            return false;
        }

        entry.setStatus(Status.EXECUTED);

        // Advance nextUnexecutedSequence if this was the next expected entry
        nextUnexecutedSequence.compareAndSet(seqNum, seqNum + 1);

        saveExecutor.submit(this::save);
        logger.info("Marked seq={} as EXECUTED. Next unexecuted: {}",
                seqNum, nextUnexecutedSequence.get());
        return true;
    }

    public LogEntry getLogEntry(long seqNum) {
        return log.get(seqNum);
    }

    public void setLogEntry(long seqNum, Ballot ballot, MessageServiceOuterClass.ClientRequest request) {
        LogEntry newLogEntry = new LogEntry(seqNum, 0, Status.ACCEPTED, ballot, request);
        log.put(seqNum, newLogEntry);
        logger.info("Set log entry at sequence number = {}", sequenceNumber);
        this.saveExecutor.submit(this::save);
    }

    public List<MessageServiceOuterClass.AcceptMessage> getAcceptLog() {

        List<MessageServiceOuterClass.AcceptMessage> acceptLog = new ArrayList<>();
        for (long i = 1; i <= sequenceNumber.get(); i++) {
            LogEntry logEntry = log.get(i);
            MessageServiceOuterClass.AcceptMessage acceptLogEntry = MessageServiceOuterClass.AcceptMessage.newBuilder()
                    .setBallot(logEntry.getBallot().toProtoBallot())
                    .setSequenceNumber(i)
                    .setRequest(logEntry.getRequest())
                    .build();
            acceptLog.add(acceptLogEntry);
        }
        return acceptLog;
    }

    public Status getStatus(long sequenceNumber) {
        return log.get(sequenceNumber).getStatus();
    }

    public boolean isAccepted(long sequenceNumber) {
        return log.get(sequenceNumber).isAccepted();
    }

    public boolean isCommitted(long sequenceNumber) {
        LogEntry entry = log.get(sequenceNumber);
        return entry != null && entry.isCommitted();
    }

    public boolean isExecuted(long sequenceNumber) {
        LogEntry entry = log.get(sequenceNumber);
        return entry != null && entry.isExecuted();
    }

    public MessageServiceOuterClass.ClientRequest getRequest(long sequenceNumber) {
        return log.get(sequenceNumber).getRequest();
    }

    public long getSequenceNumber() {
        return sequenceNumber.get();
    }

    public int size() {
        return log.size();
    }

    public void print() {
        System.out.println("------------------------------------");
        System.out.println("SERVER " + serverId + " LOG");
        System.out.println("------------------------------------");
        for (long i = 1; i <= sequenceNumber.get(); i++) {
            System.out.println(log.get(i));
        }
    }

}
