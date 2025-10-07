package org.example;

import io.grpc.ManagedChannel;

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

//        String serverId = "n1";

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (String serverId : Config.getServerIds()) {
            setServerNodeActiveFlag(serverId, true);
        }

        ManagedChannel channel = ChannelManager.createOrGetChannel("n2");
        MessageServiceGrpc.MessageServiceBlockingStub stub = MessageServiceGrpc.newBlockingStub(channel);

        // create request here
        MessageServiceOuterClass.ClientRequest request = MessageServiceOuterClass.ClientRequest.newBuilder().setClientId("A").setTimestamp(System.currentTimeMillis()).build();

        // call rpc here
        try {
            System.out.println("Sending request. Waiting for reply...");
            MessageServiceOuterClass.ClientReply reply = stub.request(request);
            System.out.println(reply);
        } catch (Exception e) {
            System.out.println("Error when communicating with server : " + e.getMessage());
        }
    }
}
