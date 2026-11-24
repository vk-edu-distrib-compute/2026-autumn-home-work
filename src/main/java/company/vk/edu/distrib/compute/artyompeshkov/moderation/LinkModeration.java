package company.vk.edu.distrib.compute.artyompeshkov.moderation;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkModeration implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LinkModeration.class);
    private static final int QUEUE_CAPACITY = 256;
    private final String bootstrapServers;
    private final Dao<String> links;
    private final KafkaSubscription subscription;
    private final BlockingQueue<LinkCreatedEvent> pendingLinks = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread sender = Thread.ofPlatform().daemon().name("link-events").unstarted(this::publishPending);

    public LinkModeration(String bootstrapServers, Dao<String> links) {
        this.bootstrapServers = bootstrapServers;
        this.links = links;
        subscription = new KafkaSubscription(bootstrapServers, "links_" + UUID.randomUUID(),
                KafkaUtils.MODERATED_TOPIC, this::applyResult);
    }

    public void start() {
        sender.start();
        subscription.start();
    }

    public void publish(String id, String url) {
        if (running.get() && !pendingLinks.offer(new LinkCreatedEvent(id, url, Instant.now()))) {
            log.warn("Moderation queue is full, link {} remains available", id);
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void publishPending() {
        try {
            LinkCreatedEvent pending = pendingLinks.take();
            while (running.get()) {
                try {
                    KafkaProducer<String, byte[]> producer = KafkaUtils.producer(bootstrapServers);
                    try {
                        while (running.get()) {
                            sendEvent(producer, pending);
                            pending = pendingLinks.take();
                        }
                    } finally {
                        producer.close(Duration.ZERO);
                    }
                } catch (ExecutionException | RuntimeException e) {
                    if (running.get()) {
                        log.warn("Cannot publish link event, retaining it for retry", e);
                        Thread.sleep(KafkaUtils.RETRY_DELAY);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sendEvent(KafkaProducer<String, byte[]> producer, LinkCreatedEvent event)
            throws InterruptedException, ExecutionException {
        producer.send(new ProducerRecord<>(KafkaUtils.CREATED_TOPIC, event.id(), EventCodec.encode(event))).get();
    }

    private void applyResult(ConsumerRecord<String, byte[]> record) {
        ModerationResultEvent result;
        try {
            result = EventCodec.decodeModerated(record.value(), record.key());
        } catch (RuntimeException e) {
            log.warn("Ignoring invalid moderation result", e);
            return;
        }
        if (!result.passed()) {
            try {
                links.delete(result.id());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        KafkaUtils.stopWorker(sender);
        subscription.close();
        pendingLinks.clear();
    }
}
