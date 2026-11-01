package company.vk.edu.distrib.compute.playingpeano.kv;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
        Outcome snapshot = Outcome.UNAVAILABLE;
        boolean finish = false;
        lock.lock();
        try {
            finished++;
            if (result.answered()) {
                answers++;
                if (result.found() && !found) {
                    found = true;
                    value = result.value();
                }
            }
            if (done.isDone()) {
                return;
            }
            boolean enough = answers >= required;
            boolean impossible = answers + (total - finished) < required;
            if (enough) {
                snapshot = found ? Outcome.FOUND : Outcome.MISSING;
                finish = true;
            } else if (impossible) {
                snapshot = Outcome.UNAVAILABLE;
                finish = true;
            }
        } finally {
            lock.unlock();
        }
        if (finish) {
            done.complete(snapshot);
        }
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
