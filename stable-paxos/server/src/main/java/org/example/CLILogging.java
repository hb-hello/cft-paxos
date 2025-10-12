package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class CLILogging {

    public static String formatLog(List<String> communicationLog) {
        // create prettified string out of list communicationLog
        StringBuilder sb = new StringBuilder();
        for (String logEntry : communicationLog) {
            sb.append(logEntry).append("\n");
        }
        return sb.toString();
    }

    public static String formatState(ClientState state) {
        // create JSON string out of hashmap state.getClientState() using jackson

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(state.getClientState());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String mapStatus(Status status) {
        return switch (status) {
            case ACCEPTED -> "A";
            case COMMITTED -> "C";
            case EXECUTED -> "E";
            default -> "X";
        };
    }

    public static String formatNewViews(List<MessageServiceOuterClass.NewViewMessage> newViews) {
        StringBuilder sb = new StringBuilder();
        for (MessageServiceOuterClass.NewViewMessage newView : newViews) {
            sb.append(String.format("\n\nNewView: ballot=%s\n",
                    Ballot.fromProtoBallot(newView.getBallot())));
            for (MessageServiceOuterClass.AcceptMessage acceptMessage : newView.getAcceptLogList()) {

                //bring in same string.format as present in communication logging
                sb.append(String.format("<ACCEPT, <%d, %s>, %d, (%s, %s, %f)>\n",
                        acceptMessage.getBallot().getInstance(),
                        acceptMessage.getBallot().getSenderId(),
                        acceptMessage.getSequenceNumber(),
                        acceptMessage.getRequest().getTransaction().getSender(),
                        acceptMessage.getRequest().getTransaction().getReceiver(),
                        acceptMessage.getRequest().getTransaction().getAmount()));
            }
        }
        return sb.toString();
    }
}
