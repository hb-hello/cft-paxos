package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigLoader {

    private static final Logger logger = LogManager.getLogger(ConfigLoader.class);

    public static Map<String, ServerDetails> loadServersFromConfig(String serverDetailsFilePath) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<ServerDetails> serverList = mapper.readValue(new File(serverDetailsFilePath), new TypeReference<>() {});
            Map<String, ServerDetails> servers = new HashMap<>();
            for (ServerDetails server : serverList) {
                servers.put(server.id(), server);
            }
            return servers;
        } catch (IOException e) {
            logger.error("Failed to load server details from config file {} : {}", serverDetailsFilePath, e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Double> loadClientDetails(String clientDetailsFilePath) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<ClientDetails> clientList = mapper.readValue(new File(clientDetailsFilePath), new TypeReference<>() {});
            Map<String, Double> clientDetails = new HashMap<>();
            for (ClientDetails client : clientList) {
                clientDetails.put(client.id(), client.startingBalance());
            }
            return clientDetails;
        } catch (IOException e) {
            logger.error("Error when loading client details to initialize state : {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
