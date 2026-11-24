package company.vk.edu.distrib.compute.k0oshara.urlshortener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

final class LinkModeration implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(LinkModeration.class.getName());
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);
    private static final Duration CLIENT_CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final long STOP_TIMEOUT_MILLIS = 4000;
    private static final long RETRY_DELAY_MILLIS = 250;
    private final String bootstrapServers;
    private final String consumerGroup = "shortener-" + UUID.randomUUID();
    private final Dao<String> links;
    private final BlockingQueue<LinkCreatedEvent> pendingEvents = new ArrayBlockingQueue<>(256);
    private final AtomicBoolean running = new AtomicBoolean();
    private final Thread publisherThread;
    private final Thread resultThread;

    LinkModeration(String bootstrapServers, Dao<String> links) {
        this.bootstrapServers = bootstrapServers;
        this.links = links;
        publisherThread = Thread.ofPlatform().daemon().name("link-moderation-publisher").unstarted(
            this::publishEvents);
        resultThread = Thread.ofPlatform().daemon().name("link-moderation-results").unstarted(
            this::consumeResults);
    }

    void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Moderation is already running");
        }
        publisherThread.start();
        resultThread.start();
    }

    void publish(String id, String url) {
        if (!pendingEvents.offer(new LinkCreatedEvent(id, url, Instant.now()))) {
            LOGGER.log(System.Logger.Level.WARNING, "Link moderation queue is full; event was dropped");
        }
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) {
            return;
        }
        pendingEvents.clear();
        publisherThread.interrupt();
        join(resultThread);
        join(publisherThread);
    }

    private void publishEvents() {
        KafkaProducer<byte[], byte[]> producer = null;
        try {
            producer = ModerationKafka.producer(bootstrapServers);
            while (running.get()) {
                send(producer, pendingEvents.take());
            }
        } catch (InterruptedException expected) {
        } catch (RuntimeException exception) {
            LOGGER.log(System.Logger.Level.WARNING, "Link moderation publisher stopped", exception);
        } finally {
            closeProducer(producer);
        }
    }

    private static void send(KafkaProducer<byte[], byte[]> producer, LinkCreatedEvent event) {
        byte[] key = event.id().getBytes(StandardCharsets.UTF_8);
        try {
            producer.send(new ProducerRecord<>(ModerationKafka.CREATED, key, ModerationKafka.encode(event)),
                (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.log(System.Logger.Level.WARNING, "Unable to publish link-created event", exception);
                    }
                });
        } catch (KafkaException exception) {
            LOGGER.log(System.Logger.Level.WARNING, "Unable to publish link-created event", exception);
        }
    }

    private void consumeResults() {
        while (running.get()) {
            KafkaConsumer<byte[], byte[]> currentConsumer = null;
            try {
                currentConsumer = ModerationKafka.consumer(bootstrapServers, consumerGroup);
                currentConsumer.subscribe(List.of(ModerationKafka.MODERATED));
                consume(currentConsumer);
            } catch (IOException | RuntimeException exception) {
                if (running.get()) {
                    LOGGER.log(System.Logger.Level.WARNING, "Unable to consume moderation results", exception);
                }
            } finally {
                closeConsumer(currentConsumer);
            }
            retry();
        }
    }

    private void consume(KafkaConsumer<byte[], byte[]> input) throws IOException {
        while (running.get()) {
            for (ConsumerRecord<byte[], byte[]> record : input.poll(POLL_TIMEOUT)) {
                if (!running.get()) {
                    return;
                }
                applyResult(record);
                input.commitSync(CLIENT_CLOSE_TIMEOUT);
            }
        }
    }

    private void applyResult(ConsumerRecord<byte[], byte[]> record) throws IOException {
        ModerationResultEvent result;
        try {
            result = ModerationKafka.decodeResult(record.key(), record.value());
        } catch (IllegalArgumentException expected) {
            return;
        }
        if (!result.passed()) {
            links.delete(result.id());
        }
    }

    private void retry() {
        try {
            Thread.sleep(RETRY_DELAY_MILLIS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeProducer(KafkaProducer<byte[], byte[]> producer) {
        if (producer != null) {
            producer.close(CLIENT_CLOSE_TIMEOUT);
        }
    }

    private static void closeConsumer(KafkaConsumer<byte[], byte[]> consumer) {
        if (consumer != null) {
            consumer.close(CloseOptions.timeout(CLIENT_CLOSE_TIMEOUT));
        }
    }

    private static void join(Thread worker) {
        try {
            worker.join(STOP_TIMEOUT_MILLIS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping moderation", exception);
        }
        if (worker.isAlive()) {
            throw new IllegalStateException("Moderation worker did not stop within the timeout");
        }
    }
}
