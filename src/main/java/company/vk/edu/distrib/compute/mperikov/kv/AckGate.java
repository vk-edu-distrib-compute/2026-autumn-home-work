package company.vk.edu.distrib.compute.mperikov.kv;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

final class AckGate {
    private static final long AWAIT_MILLIS = 1_500L;

    private final int total;
    private final int needed;
    private final CompletableFuture<Integer> done = new CompletableFuture<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int success;
    private int finished;

    AckGate(int total, int needed) {
        this.total = total;
        this.needed = needed;
    }

    void settle(@Nullable Boolean ok, @Nullable Throwable error) {
        lock.lock();
        try {
            finished++;
            if (error == null && Boolean.TRUE.equals(ok)) {
                success++;
            }
            if (!done.isDone() && decided()) {
                done.complete(success);
            }
        } finally {
            lock.unlock();
        }
    }

    int await() {
        try {
            return done.get(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return snapshot();
        } catch (ExecutionException | TimeoutException ex) {
            ReplicaClient.miss(ex);
            return snapshot();
        }
    }

    private boolean decided() {
        return success >= needed || success + (total - finished) < needed;
    }

    private int snapshot() {
        lock.lock();
        try {
            return success;
        } finally {
            lock.unlock();
        }
    }
}
