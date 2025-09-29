package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigLoader {

    public static HashMap<String, ServerDetails> loadServersFromConfig(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<ServerDetails> serverList = mapper.readValue(new File(filePath), new TypeReference<>() {});
        Map<String, ServerDetails> servers = new HashMap<>();
        for (ServerDetails server : serverList) {
            servers.put(server.id(), server);
        }
        return new HashMap<>(servers);
    }

    public static Map<String, Double> loadClientDetails(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<ClientDetails> clientList = mapper.readValue(new File(filePath), new TypeReference<>() {});
        Map<String, Double> clientDetails = new HashMap<>();
        for (ClientDetails client : clientList) {
            clientDetails.put(client.id(), client.startingBalance());
        }
        return clientDetails;
    }

}
