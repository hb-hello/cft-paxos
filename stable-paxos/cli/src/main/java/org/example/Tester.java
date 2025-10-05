package org.example;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.sql.Timestamp;

import static org.example.ChannelManager.createOrGetChannel;

public class Tester {

    private static void setServerNodeActiveFlag(String serverId, boolean activeFlag) {
        try {
            ManagedChannel channel = createOrGetChannel(serverId);
            MessageServiceGrpc.MessageServiceBlockingStub stub = MessageServiceGrpc.newBlockingStub(channel);
            MessageServiceOuterClass.Acknowledgement ack = stub.setActiveFlag(MessageServiceOuterClass.ActiveFlag.newBuilder().setActiveFlag(activeFlag).build());
            if (!ack.getStatus()) {
                System.out.printf("Server %s not activated%n", serverId);
                throw new RuntimeException("Server {} not activated");
            } else System.out.printf("Server %s activated%n", serverId);
        } catch (RuntimeException e) {
            System.out.printf("Error when activating server %s.%n", serverId);
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {

        Config.initialize();

        String serverId = "n1";

        setServerNodeActiveFlag(serverId, true);

        ManagedChannel channel = ChannelManager.createOrGetChannel(serverId);
        MessageServiceGrpc.MessageServiceBlockingStub stub = MessageServiceGrpc.newBlockingStub(channel);

        // create request here
        MessageServiceOuterClass.ClientRequest request = MessageServiceOuterClass.ClientRequest.newBuilder().setClientId("A").setTimestamp(System.currentTimeMillis()).build();

        // call rpc here
        try {
            MessageServiceOuterClass.ClientReply reply = stub.request(request);
            System.out.println(reply);
        } catch (Exception e) {
            System.out.println("Error when communicating with server : " + e.getMessage());
        }
    }

}
