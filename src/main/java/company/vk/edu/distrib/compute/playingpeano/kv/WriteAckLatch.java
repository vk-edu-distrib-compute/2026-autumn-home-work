package company.vk.edu.distrib.compute.playingpeano.kv;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class WriteAckLatch {
    private final int required;
    private final int total;
    private final CompletableFuture<Integer> done = new CompletableFuture<>();
    private final Lock lock = new ReentrantLock();
    private int successes;
    private int finished;

    WriteAckLatch(int required, int total) {
        this.required = required;
        this.total = total;
    }

    void record(boolean success) {
        int snapshot = 0;
        boolean finish = false;
        lock.lock();
        try {
            finished++;
            if (success) {
                successes++;
            }
            if (done.isDone()) {
                return;
            }
            boolean enough = successes >= required;
            boolean impossible = successes + (total - finished) < required;
            if (enough || impossible) {
                snapshot = successes;
                finish = true;
            }
        } finally {
            lock.unlock();
        }
        if (finish) {
            done.complete(snapshot);
        }
    }

    int await() {
        return done.join();
    }
}
