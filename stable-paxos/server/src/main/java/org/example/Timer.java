package org.example;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Timer {

    private final long timeoutMillis;
    private final ScheduledExecutorService scheduler;
    private final Runnable callback;
    private ScheduledFuture<?> scheduledFuture;
    private AtomicBoolean isRunning;

    public Timer(long timeoutMillis, Runnable callback) {
        this.timeoutMillis = timeoutMillis;
        this.callback = callback;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.isRunning = new AtomicBoolean(false);
    }

    public void start() {
        try {
            isRunning.set(true);
            scheduledFuture = scheduler.schedule(() -> {
                        isRunning.set(false);
                        callback.run();
                    },
                    timeoutMillis,
                    TimeUnit.MILLISECONDS
            );
        } catch (Exception e) {
            throw new RuntimeException("Timer errored out : " + e.getMessage());
        } finally {
            scheduler.shutdown();
        }
    }

    /**
     * Starts or resets the timer.
     */
    public synchronized void startOrRefresh() {
        // Cancel any existing task
        if (scheduledFuture != null && !scheduledFuture.isDone()) {
            scheduledFuture.cancel(false);
        }
        // Schedule a new task
        start();
    }

    public synchronized void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
        isRunning.set(false);
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
