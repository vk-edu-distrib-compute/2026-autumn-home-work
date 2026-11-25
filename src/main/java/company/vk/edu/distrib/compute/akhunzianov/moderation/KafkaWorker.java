package company.vk.edu.distrib.compute.akhunzianov.moderation;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaWorker.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(2);
    private static final long RETRY_DELAY_MS = 100;
    private static final long STOP_TIMEOUT_MS = 2500;
    private static final long GRACEFUL_STOP_MS = 500;

    private final KafkaConsumer<String, byte[]> consumer;
    private final Handler handler;
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public KafkaWorker(String bootstrapServers, String group, String topic, Handler handler) {
        this.consumer = new KafkaConsumer<>(KafkaSettings.consumer(bootstrapServers, group));
        this.handler = handler;
        consumer.subscribe(List.of(topic));
        thread = Thread.ofPlatform().daemon().name(group).unstarted(this::run);
    }

    public void start() {
        thread.start();
    }

    private void run() {
        try {
            while (running.get()) {
                poll();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeConsumer();
        }
    }

    private void poll() throws InterruptedException {
        try {
            for (var record : consumer.poll(POLL_TIMEOUT)) {
                process(record);
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } catch (KafkaException e) {
            if (running.get()) {
                if (log.isWarnEnabled()) {
                    log.warn("Kafka consumer will retry", e);
                }
                Thread.sleep(RETRY_DELAY_MS);
            }
        }
    }

    private void closeConsumer() {
        boolean interrupted = Thread.interrupted();
        try {
            consumer.close(Duration.ofMillis(GRACEFUL_STOP_MS));
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void process(ConsumerRecord<String, byte[]> record) throws InterruptedException {
        var partition = new TopicPartition(record.topic(), record.partition());
        try {
            handler.handle(record);
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)), COMMIT_TIMEOUT);
        } catch (IOException | ExecutionException | RuntimeException e) {
            if (!running.get()) {
                return;
            }
            consumer.seek(partition, record.offset());
            if (running.get()) {
                if (log.isWarnEnabled()) {
                    log.warn("Cannot process record {}:{}:{}", record.topic(), record.partition(), record.offset(), e);
                }
                Thread.sleep(RETRY_DELAY_MS);
            }
        }
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) {
            return;
        }
        consumer.wakeup();
        try {
            thread.join(GRACEFUL_STOP_MS);
            if (thread.isAlive()) {
                thread.interrupt();
                thread.join(STOP_TIMEOUT_MS);
            }
            if (thread.isAlive()) {
                throw new IllegalStateException("Kafka worker did not stop");
            }
        } catch (InterruptedException e) {
            thread.interrupt();
            Thread.currentThread().interrupt();
        }
    }

    @FunctionalInterface
    public interface Handler {
        void handle(ConsumerRecord<String, byte[]> record) throws IOException, InterruptedException, ExecutionException;
    }
}
