package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class Quorum {
    private final int ack;
    private final int maxFailures;
    private final CompletableFuture<Boolean> reached = new CompletableFuture<>();
    private int successes;
    private int failures;
    private byte @Nullable [] value;

    Quorum(int ack, int replicas) {
        this.ack = ack;
        this.maxFailures = replicas - ack;
    }

    synchronized void success(byte @Nullable [] found) {
        successes++;
        if (value == null && found != null) {
            value = found.clone();
        }
        if (successes >= ack) {
            reached.complete(true);
        }
    }

    synchronized void failure() {
        failures++;
        if (failures > maxFailures) {
            reached.complete(false);
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

    synchronized byte @Nullable [] value() {
        return value == null ? null : value.clone();
    }
}
