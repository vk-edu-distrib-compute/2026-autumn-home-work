package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

final class Quorum {
    private final int ack;
    private final int maxFailures;
    private final CompletableFuture<Boolean> reached = new CompletableFuture<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int successes;
    private int failures;
    private byte @Nullable [] value;

    Quorum(int ack, int replicas) {
        this.ack = ack;
        this.maxFailures = replicas - ack;
    }

    void success(byte @Nullable [] found) {
        lock.lock();
        try {
            successes++;
            if (value == null && found != null) {
                value = found.clone();
            }
            if (successes >= ack) {
                reached.complete(true);
            }
        } finally {
            lock.unlock();
        }
    }

    void failure() {
        lock.lock();
        try {
            failures++;
            if (failures > maxFailures) {
                reached.complete(false);
            }
        } finally {
            lock.unlock();
        }
    }

    boolean await(long timeoutMillis) {
        try {
            return reached.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        }
    }

    byte @Nullable [] value() {
        lock.lock();
        try {
            return value == null ? null : value.clone();
        } finally {
            lock.unlock();
        }
    }
}
