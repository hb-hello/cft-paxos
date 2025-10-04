package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Log {
    private static Logger logger = LogManager.getLogger(Log.class);
    private final Map<Long, LogEntry> log;
    private final long startingSequenceNumber;
    private String serverId;

    public Log(String serverId) {

        try {
            Map<Long, LogEntry> log = loadLog();
            if (log == null) {
                this.log = new HashMap<>();
                this.startingSequenceNumber = 1;
            } else {
                this.log = log;
                this.startingSequenceNumber = Collections.min(log.keySet());
            }
        } catch (Exception e) {
            logger.error("Server {} : Error initializing log", serverId);
            throw new RuntimeException(e);
        }
    }

    private void saveLog() {
        try {
            LogLoader.saveLogEntries(serverId, log);
        } catch (Exception e) {
            logger.error("Server {} : Error when saving log to file", serverId);
            throw new RuntimeException(e);
        }
    }

    private Map<Long, LogEntry> loadLog() {
        try {
            return LogLoader.loadLogEntries(serverId);
        } catch (Exception e) {
            logger.error("Server {} : Error when loading log from file", serverId);
            throw new RuntimeException(e);
        }
    }

//    Add function to save the log to a JSON file, another to load it from the JSON file
}
