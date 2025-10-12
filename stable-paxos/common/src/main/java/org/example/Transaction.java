package org.example;

public class Transaction implements TransactionEvent {
    private final String sender;
    private final String receiver;
    private final double amount;

    public Transaction(String sender, String receiver, double amount) {
        this.sender = sender;
        this.receiver = receiver;
        this.amount = amount;
    }

    public String getSender() { return sender; }
    public String getReceiver() { return receiver; }
    public double getAmount() { return amount; }

    @Override
    public String getEventType() { return "TRANSACTION"; }

    @Override
    public String toString() {
        return String.format("Transaction(%s -> %s: %.0f)", sender, receiver, amount);
    }

    public MessageServiceOuterClass.Transaction toProtoTransaction() {
        return MessageServiceOuterClass.Transaction.newBuilder()
                .setSender(sender)
                .setReceiver(receiver)
                .setAmount(amount)
                .build();
    }

    public static Transaction fromProtoTransaction(MessageServiceOuterClass.Transaction proto) {
        return new Transaction(proto.getSender(), proto.getReceiver(), proto.getAmount());
    }
}
