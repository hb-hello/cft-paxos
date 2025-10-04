package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogLoader {

    private static final Logger logger = LogManager.getLogger(LogLoader.class);
    private static final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final String FILE_PATH_PREFIX = "C:\\Users\\hbiyani\\OneDrive - Stony Brook University\\Documents\\DEV\\cft-hb-hello\\stable-paxos\\data\\server-n";
    private static final String FILE_PATH_SUFFIX = "-log.json";

    /**
     * Save a list of LogEntry records to a JSON file
     */
    public static void saveLogEntries(String serverId, Map<Long, LogEntry> logEntries) {

        String filePath = FILE_PATH_PREFIX + serverId + FILE_PATH_SUFFIX;

        try {
            ArrayNode jsonArray = mapper.createArrayNode();

            for (LogEntry entry : logEntries.values()) {
                ObjectNode entryNode = mapper.createObjectNode();
                entryNode.put("sequenceNumber", entry.sequenceNumber());
                entryNode.put("acceptedVotes", entry.acceptedVotes());
                entryNode.put("status", entry.status().name());

                // Convert protobuf Transaction to JSON string
                String transactionJson = JsonFormat.printer().alwaysPrintFieldsWithNoPresence().print(entry.transaction());

                // Parse the transaction JSON and add as nested object
                JsonNode transactionNode = mapper.readTree(transactionJson);
                entryNode.set("transaction", transactionNode);

                jsonArray.add(entryNode);
            }

            mapper.writeValue(new File(filePath), jsonArray);
            logger.info("Successfully saved {} log entries to {}", logEntries.size(), filePath);
        } catch (IOException e) {
            logger.error("Failed to save log entries to file {}: {}", filePath, e.getMessage());
            throw new RuntimeException("Failed to save log entries", e);
        }
    }

    /**
     * Load a list of LogEntry records from a JSON file
     */
    public static Map<Long, LogEntry> loadLogEntries(String serverId) {

        String filePath = FILE_PATH_PREFIX + serverId + FILE_PATH_SUFFIX;

        try {
            JsonNode rootNode = mapper.readTree(new File(filePath));
            Map<Long, LogEntry> logEntries = new HashMap<>();

            if (!rootNode.isArray()) {
                throw new IllegalArgumentException("JSON file must contain an array of log entries");
            }

            for (JsonNode entryNode : rootNode) {
                long sequenceNumber = entryNode.get("sequenceNumber").asLong();
                int acceptedVotes = entryNode.get("acceptedVotes").asInt();
                Status status = Status.valueOf(entryNode.get("status").asText());

                // Convert transaction JSON back to protobuf
                JsonNode transactionNode = entryNode.get("transaction");
                String transactionJson = mapper.writeValueAsString(transactionNode);

                MessageServiceOuterClass.Transaction.Builder builder = MessageServiceOuterClass.Transaction.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(transactionJson, builder);

                logEntries.put(sequenceNumber, new LogEntry(sequenceNumber, acceptedVotes, status, builder.build()));
            }

            logger.info("Server {} : Successfully loaded {} log entries from {}", serverId, logEntries.size(), filePath);
            return logEntries;
        } catch (IOException e) {
            logger.error("Server {} : Failed to load log entries from file {}: {}", serverId, filePath, e.getMessage());
            return null;
        }
    }
}