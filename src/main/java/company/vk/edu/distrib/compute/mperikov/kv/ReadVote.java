package company.vk.edu.distrib.compute.mperikov.kv;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

final class ReadVote {
    private static final long AWAIT_MILLIS = 1_500L;

    private final int needed;
    private final int total;
    private final CompletableFuture<ReadResult> done = new CompletableFuture<>();
    private final ReentrantLock lock = new ReentrantLock();
    private int answers;
    private int finished;
    private boolean present;
    private byte[] stored = new byte[0];

    ReadVote(int needed, int total) {
        this.needed = needed;
        this.total = total;
    }

    void remember(int status, byte[] body) {
        if (status == KvCodes.OK) {
            addAnswer(true, body);
            return;
        }
        if (status == KvCodes.NOT_FOUND) {
            addAnswer(false, body);
            return;
        }
        failed();
    }

    void failed() {
        lock.lock();
        try {
            finished++;
            publish();
        } finally {
            lock.unlock();
        }
    }

    ReadResult await() {
        if (total == 0) {
            return new ReadResult(0, false, new byte[0]);
        }
        try {
            return done.get(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return current();
        } catch (ExecutionException | TimeoutException ex) {
            ReplicaClient.miss(ex);
            return current();
        }
    }

    private void addAnswer(boolean hit, byte[] body) {
        lock.lock();
        try {
            finished++;
            answers++;
            if (hit) {
                keep(body);
            }
            publish();
        } finally {
            lock.unlock();
        }
    }

    private void keep(byte[] body) {
        if (present) {
            return;
        }
        present = true;
        stored = body.clone();
    }

    private void publish() {
        if (done.isDone() || !ready()) {
            return;
        }
        done.complete(new ReadResult(answers, present, stored));
    }

    private boolean ready() {
        return answers >= needed || answers + (total - finished) < needed;
    }

    private ReadResult current() {
        lock.lock();
        try {
            return new ReadResult(answers, present, stored);
        } finally {
            lock.unlock();
        }
    }

    record ReadResult(int answers, boolean present, byte[] stored) {
    }
}
