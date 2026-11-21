package company.vk.edu.distrib.compute.cloudyy74.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.cloudyy74.moderation.Cloudyy74KafkaUtils;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils.UncheckedCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class Cloudyy74ModerationClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Cloudyy74ModerationClient.class);
    private static final int QUEUE_CAPACITY = 1000;

    private final Dao<String> linksDao;
    private final BlockingQueue<LinkCreatedEvent> events = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final Thread worker = new Thread(this::exchangeEvents, "cloudyy74-shortener-kafka");
    private final AtomicBoolean running = new AtomicBoolean();

    Cloudyy74ModerationClient(String bootstrapServers, Dao<String> linksDao) {
        this.linksDao = linksDao;
        producer = Cloudyy74KafkaUtils.createProducer(bootstrapServers);
        consumer = Cloudyy74KafkaUtils.createConsumer(bootstrapServers, "cloudyy74-shortener-" + UUID.randomUUID());
    }

    void start() {
        running.set(true);
        worker.start();
    }

    void publish(String id, String url) {
        if (!events.offer(new LinkCreatedEvent(id, url, Instant.now()))) {
            log.warn("Moderation queue is full, skipping link {}", id);
        }
    }

    private void exchangeEvents() {
        try (UncheckedCloseable _ = () -> producer.close(Cloudyy74KafkaUtils.CLOSE_TIMEOUT);
             UncheckedCloseable _ = () -> consumer.close(Cloudyy74KafkaUtils.CLOSE_TIMEOUT)) {
            consumer.subscribe(List.of(Cloudyy74KafkaUtils.MODERATED_TOPIC));
            while (running.get()) {
                sendCreatedLinks();
                for (var record : consumer.poll(Cloudyy74KafkaUtils.POLL_TIMEOUT)) {
                    try {
                        final var result = Cloudyy74KafkaUtils.readModerated(record.value());
                        if (!result.passed()) {
                            linksDao.delete(result.id());
                        }
                    } catch (IOException | IllegalArgumentException e) {
                        log.error("Failed to apply moderation result", e);
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Moderation result consumer was interrupted", e);
            }
        } catch (KafkaException e) {
            log.error("Failed to receive moderation results", e);
        }
    }

    private void sendCreatedLinks() {
        while (running.get()) {
            final var event = events.poll();
            if (event == null) {
                return;
            }
            sendCreatedLink(event);
        }
    }

    private void sendCreatedLink(LinkCreatedEvent event) {
        final var record = new ProducerRecord<>(Cloudyy74KafkaUtils.CREATED_TOPIC,
                event.id(), Cloudyy74KafkaUtils.serialize(event));
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.warn("Failed to send link for moderation", exception);
                }
            });
        } catch (KafkaException e) {
            log.warn("Failed to send link for moderation", e);
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
