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
    private final long firstSequenceNumber; // maintains last stable checkpoint
    private final AtomicLong sequenceNumber; // thread-safe sequence number
    private final AtomicLong lastSavedSequenceNumber;
    private final String serverId;

    public Log(String serverId) {

        this.serverId = serverId;
        this.lastSavedSequenceNumber = new AtomicLong(0);
        this.saveExecutor = Executors.newSingleThreadExecutor();

        try {
            this.log = new ConcurrentHashMap<>();
            this.firstSequenceNumber = 0;
            this.sequenceNumber = new AtomicLong(0);
        } catch (Exception e) {
            logger.error("Server {} : Error initializing log", serverId);
            throw new RuntimeException(e);
        }
    }

    private void save() {
        try {
            long currentSeq = sequenceNumber.get();
            long lastSaved = lastSavedSequenceNumber.get();

            if (currentSeq > lastSaved) {
                Map<Long, LogEntry> snapshot = new HashMap<>(log);
                LogLoader.saveLogEntries(serverId, snapshot);
                lastSavedSequenceNumber.set(currentSeq);
                logger.info("Saved log up to sequence: {}", currentSeq);
            }
        } catch (Exception e) {
            logger.error("Error during log save: {}", e.getMessage());
        }
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
        long seqNum = sequenceNumber.incrementAndGet();
        LogEntry logEntry = new LogEntry(seqNum, 0, Status.ACCEPTED, ballot, request);
        log.put(seqNum, logEntry);

//        trigger async save
        this.saveExecutor.submit(this::save);
        return seqNum;
    }

    public synchronized List<MessageServiceOuterClass.AcceptMessage> getAcceptLog() {

        List<MessageServiceOuterClass.AcceptMessage> acceptLog = new ArrayList<>();
        for (long i = 1; i <= sequenceNumber.get(); i++) {
            LogEntry logEntry = log.get(i);
            MessageServiceOuterClass.AcceptMessage acceptLogEntry = MessageServiceOuterClass.AcceptMessage.newBuilder()
                    .setBallot(logEntry.ballot().toProtoBallot())
                    .setSequenceNumber(i)
                    .setRequest(logEntry.request())
                    .build();
            acceptLog.add(acceptLogEntry);
        }
        return acceptLog;
    }

    public Status getStatus(long sequenceNumber) {
        return log.get(sequenceNumber).status();
    }

    public MessageServiceOuterClass.ClientRequest getRequest(long sequenceNumber) {
        return log.get(sequenceNumber).request();
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
