package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateLoader {

    private static final String FILE_PATH_PREFIX = "src/main/resources/clientState-node-";
    private static final String FILE_PATH_SUFFIX = ".json";

    public static void saveState(String serverId, Map<String, ClientRecord> clientStates) throws IOException {
        String filePath = FILE_PATH_PREFIX + serverId + FILE_PATH_SUFFIX;
        ObjectWriter writer = new ObjectMapper().writer().withDefaultPrettyPrinter();
        writer.writeValue(new File(filePath), clientStates.values());
    }

    public static Map<String, ClientRecord> loadState(String serverId) throws IOException {
        String filePath = FILE_PATH_PREFIX + serverId + FILE_PATH_SUFFIX;

        if (!new File(filePath).isFile()) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();
        List<ClientRecord> clientRecordList = mapper.readValue(new File(filePath), new TypeReference<>() {});

        Map<String, ClientRecord> clientStates = new HashMap<>();
        for (ClientRecord clientRecord : clientRecordList) {
            clientStates.put(clientRecord.clientId(), clientRecord);
        }
        return clientStates;
    }
}
