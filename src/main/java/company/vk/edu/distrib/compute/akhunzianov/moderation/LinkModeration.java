package company.vk.edu.distrib.compute.akhunzianov.moderation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LinkModeration implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LinkModeration.class);
    private static final int QUEUE_CAPACITY = 1024;

    private final Dao<String> links;
    private final KafkaProducer<String, byte[]> producer;
    private final KafkaWorker worker;
    private final ThreadPoolExecutor sender = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(QUEUE_CAPACITY), Thread.ofPlatform().daemon().name("link-events").factory());

    public LinkModeration(String bootstrapServers, Dao<String> links) {
        this.links = links;
        producer = new KafkaProducer<>(KafkaSettings.producer(bootstrapServers));
        try {
            worker = new KafkaWorker(bootstrapServers, "akhunzianov-shortener-" + UUID.randomUUID(),
                KafkaSettings.MODERATED_TOPIC, this::apply);
            worker.start();
        } catch (RuntimeException e) {
            producer.close(Duration.ZERO);
            sender.shutdownNow();
            throw e;
        }
    }

    public void created(String id, String url) {
        var event = new LinkCreatedEvent(id, url, Instant.now());
        try {
            sender.execute(() -> publish(event));
        } catch (RejectedExecutionException e) {
            if (log.isWarnEnabled()) {
                log.warn("Moderation queue is full or stopped; keeping link {}", id);
            }
        }
    }

    private void publish(LinkCreatedEvent event) {
        try {
            var record = new ProducerRecord<>(KafkaSettings.CREATED_TOPIC, event.id(),
                EventCodec.encode(event).getBytes(StandardCharsets.UTF_8));
            producer.send(record,
                (metadata, failure) -> {
                    if (failure != null && log.isWarnEnabled()) {
                        log.warn("Cannot publish link {} for moderation", event.id(), failure);
                    }
                });
        } catch (RuntimeException e) {
            if (log.isWarnEnabled()) {
                log.warn("Cannot publish link {} for moderation", event.id(), e);
            }
        }
    }

    private void apply(ConsumerRecord<String, byte[]> record) throws IOException {
        ModerationResultEvent result;
        try {
            result = EventCodec.moderated(new String(Objects.requireNonNull(record.value(), "Missing result value"),
                StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            if (log.isWarnEnabled()) {
                log.warn("Ignoring malformed moderation result", e);
            }
            return;
        }
        if (!result.id().equals(record.key())) {
            if (log.isWarnEnabled()) {
                log.warn("Ignoring moderation result with mismatched key");
            }
            return;
        }
        if (Boolean.FALSE.equals(result.passed())) {
            try {
                links.get(result.id());
                links.delete(result.id());
            } catch (NoSuchElementException expected) {
            }
        }
    }

    @Override
    public void close() {
        sender.shutdownNow();
        worker.close();
        producer.close(Duration.ZERO);
        try {
            if (!sender.awaitTermination(1, TimeUnit.SECONDS) && log.isWarnEnabled()) {
                log.warn("Link event sender did not stop in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
