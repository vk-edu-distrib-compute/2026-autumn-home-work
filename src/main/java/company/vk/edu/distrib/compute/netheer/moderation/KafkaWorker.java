package company.vk.edu.distrib.compute.netheer.moderation;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

final class KafkaWorker {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaWorker.class);

    private final Thread thread;
    private final AtomicBoolean stopping = new AtomicBoolean();

    KafkaWorker(String name, Runnable task) {
        this.thread = Thread.ofPlatform().daemon().name(name).unstarted(() -> run(task));
    }

    void start() {
        thread.start();
    }

    void stop() {
        stopping.set(true);
        thread.interrupt();
        boolean interrupted = false;
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void run(Runnable task) {
        while (!stopping.get()) {
            try {
                task.run();
                return;
            } catch (KafkaException | UncheckedIOException exception) {
                if (!stopping.get()) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Kafka worker {} failed, retrying", thread.getName(), exception);
                    }
                    try {
                        Thread.sleep(Duration.ofSeconds(1));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }
}
