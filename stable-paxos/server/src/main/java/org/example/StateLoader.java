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

    private static final String FILE_PATH_PREFIX = "C:\\Users\\hbiyani\\OneDrive - Stony Brook University\\Documents\\DEV\\cft-hb-hello\\stable-paxos\\data\\server-";
    private static final String FILE_PATH_SUFFIX = "\\client-state.json";

    public static void saveState(String serverId, Map<String, StateEntry> clientStates) throws IOException {
        String filePath = FILE_PATH_PREFIX + serverId + FILE_PATH_SUFFIX;
        ObjectWriter writer = new ObjectMapper().writer().withDefaultPrettyPrinter();
        writer.writeValue(new File(filePath), clientStates.values());
    }

//    public static Map<String, StateEntry> loadState(String serverId) throws IOException {
//        String filePath = FILE_PATH_PREFIX + serverId + FILE_PATH_SUFFIX;
//
//        if (!new File(filePath).isFile()) {
//            return null;
//        }
//
//        ObjectMapper mapper = new ObjectMapper();
//        List<StateEntry> stateEntryList = mapper.readValue(new File(filePath), new TypeReference<>() {});
//
//        Map<String, StateEntry> clientStates = new HashMap<>();
//        for (StateEntry stateEntry : stateEntryList) {
//            clientStates.put(stateEntry.getClientId(), stateEntry);
//        }
//        return clientStates;
//    }
}
