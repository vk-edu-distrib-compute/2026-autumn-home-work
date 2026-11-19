package company.vk.edu.distrib.compute.fedorkhokhryakov.moderation;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class PollingWorker<K, V> {
    private static final Logger log = LoggerFactory.getLogger(PollingWorker.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(500);
    private static final Duration JOIN_TIMEOUT = Duration.ofSeconds(3);

    private final Consumer<K, V> consumer;
    private final String topic;
    private final RecordsHandler<K, V> handler;
    private final boolean manualCommit;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread thread;

    public PollingWorker(
        String name,
        Consumer<K, V> consumer,
        String topic,
        RecordsHandler<K, V> handler,
        boolean manualCommit
    ) {
        this.consumer = consumer;
        this.topic = topic;
        this.handler = handler;
        this.manualCommit = manualCommit;
        this.thread = Thread.ofPlatform().name(name).daemon().unstarted(this::run);
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup();
        try {
            if (!thread.join(JOIN_TIMEOUT) && log.isWarnEnabled()) {
                log.warn("Worker {} did not stop in {}", thread.getName(), JOIN_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        try {
            consumer.subscribe(List.of(topic));
            while (running.get()) {
                try {
                    pollOnce();
                } catch (WakeupException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Worker {} woken up", thread.getName());
                    }
                    break;
                } catch (KafkaException e) {
                    log.warn("Failed to process records from {}", topic, e);
                }
            }
        } finally {
            closeConsumer();
        }
    }

    private void pollOnce() {
        ConsumerRecords<K, V> records = consumer.poll(POLL_TIMEOUT);
        if (records.isEmpty()) {
            return;
        }
        handler.handle(records);
        if (manualCommit) {
            consumer.commitSync();
        }
    }

    private void closeConsumer() {
        try {
            consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
        } catch (KafkaException e) {
            log.warn("Failed to close consumer of {}", topic, e);
        }
    }

    @FunctionalInterface
    public interface RecordsHandler<K, V> {
        void handle(ConsumerRecords<K, V> records);
    }
}
