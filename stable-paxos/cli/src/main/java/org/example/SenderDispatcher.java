package org.example;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public final class SenderDispatcher implements AutoCloseable {
    public static final String LF_SENDER = "LF";

    private final Map<String, ExecutorService> executors = new ConcurrentHashMap<>();
    private final Map<String, Client> clients = new ConcurrentHashMap<>();
    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();

    public SenderDispatcher() {
        for (char c = 'A'; c <= 'J'; c++) {
            String id = String.valueOf(c);
            executors.put(id, newSingle("sender-" + id));
            clients.put(id, new Client(id));
        }
        executors.put(LF_SENDER, newSingle("sender-LF"));
    }

    private static ExecutorService newSingle(String name) {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setName(name);
            t.setDaemon(true);
            return t;
        });
    }

    public void submit(TransactionEvent event) {
        String sender = (event instanceof Transaction tx) ? tx.getSender() : LF_SENDER;
        ExecutorService ex = executors.get(sender);
        if (ex == null) throw new IllegalStateException("No executor for sender " + sender);
        submitted.incrementAndGet();
        ex.execute(() -> {
            try {
                if (event instanceof Transaction tx) {
                    clients.get(tx.getSender()).processTransaction(tx.toProtoTransaction());
                } else {
                    Client.broadcastFailLeader();
                }
            } finally {
                completed.incrementAndGet();
            }
        });
    }

    public Status snapshotStatus() {
        long s = submitted.get();
        long c = completed.get();
        return new Status(s, c, Math.max(0, s - c));
    }

    @Override
    public void close() {
        executors.values().forEach(ExecutorService::shutdown);
    }

    public record Status(long submitted, long completed, long outstanding) {
    }
}
