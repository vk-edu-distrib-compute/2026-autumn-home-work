package company.vk.edu.distrib.compute.playingpeano.kv;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

final class GetAckLatch {
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final int required;
    private final int total;
    private final CompletableFuture<Outcome> done = new CompletableFuture<>();
    private final Lock lock = new ReentrantLock();
    private int answers;
    private int finished;
    private byte[] value = EMPTY_VALUE;
    private boolean found;

    GetAckLatch(int required, int total) {
        this.required = required;
        this.total = total;
    }

    void record(ReplicaClient.GetResult result) {
        @Nullable Outcome snapshot;
        lock.lock();
        try {
            finished++;
            remember(result);
            snapshot = decision();
        } finally {
            lock.unlock();
        }
        if (snapshot != null) {
            done.complete(snapshot);
        }
    }

    private void remember(ReplicaClient.GetResult result) {
        if (!result.answered()) {
            return;
        }
        answers++;
        if (result.found() && !found) {
            found = true;
            value = result.value();
        }
    }

    private @Nullable Outcome decision() {
        if (done.isDone()) {
            return null;
        }
        if (answers >= required) {
            return foundOutcome();
        }
        if (answers + (total - finished) < required) {
            return Outcome.UNAVAILABLE;
        }
        return null;
    }

    private Outcome foundOutcome() {
        if (found) {
            return Outcome.FOUND;
        }
        return Outcome.MISSING;
    }

    Outcome await() {
        return done.join();
    }

    byte[] value() {
        return Arrays.copyOf(value, value.length);
    }

    enum Outcome {
        FOUND,
        MISSING,
        UNAVAILABLE
    }
}
