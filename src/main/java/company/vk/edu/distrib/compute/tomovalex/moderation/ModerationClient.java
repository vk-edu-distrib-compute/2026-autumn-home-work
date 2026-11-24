package company.vk.edu.distrib.compute.tomovalex.moderation;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ModerationClient {
    private static final Logger log = LoggerFactory.getLogger(ModerationClient.class);
    private static final String GROUP_PREFIX = "tomovalex-shortener-";
    private static final Duration RETRY_DELAY = Duration.ofMillis(100);

    private final Dao<String> linksDao;
    private final KafkaConsumer<String, byte[]> consumer;
    private final KafkaProducer<String, byte[]> producer;
    private final BlockingQueue<LinkCreatedEvent> events = new LinkedBlockingQueue<>();
    private final Thread sender;
    private final Thread receiver;
    private final AtomicBoolean running = new AtomicBoolean();

    public ModerationClient(String bootstrapServers, Dao<String> linksDao) {
        this.linksDao = linksDao;
        producer = KafkaUtils.createProducer(bootstrapServers);
        consumer = KafkaUtils.createConsumer(bootstrapServers, GROUP_PREFIX + UUID.randomUUID());
        sender = Thread.ofPlatform().name("tomovalex-moderation-sender").unstarted(this::sendEvents);
        receiver = Thread.ofPlatform().name("tomovalex-moderation-receiver").unstarted(this::receiveResults);
    }

    public void start() {
        running.set(true);
        receiver.start();
        sender.start();
    }

    public void stop() {
        running.set(false);
        consumer.wakeup();
        sender.interrupt();
        receiver.interrupt();
        try {
            sender.join();
            receiver.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void sendCreated(String id, String url) {
        events.add(new LinkCreatedEvent(id, url, Instant.now()));
    }

    private void receiveResults() {
        try {
            consumer.subscribe(List.of(KafkaUtils.MODERATED_TOPIC));
            while (running.get()) {
                consume();
            }
        } finally {
            KafkaUtils.releaseConsumer(consumer);
        }
    }

    private void sendEvents() {
        try {
            while (running.get()) {
                sendEvent(events.take());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            KafkaUtils.releaseProducer(producer);
        }
    }

    private void sendEvent(LinkCreatedEvent event) throws InterruptedException {
        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(KafkaUtils.CREATED_TOPIC, event.id(), EventUtils.serialize(event));
        while (running.get()) {
            try {
                producer.send(record).get();
                return;
            } catch (ExecutionException | KafkaException e) {
                if (running.get()) {
                    log.warn("Could not send link for moderation", e);
                    Thread.sleep(RETRY_DELAY);
                }
            }
        }
    }

    private void consume() {
        try {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(KafkaUtils.POLL_TIMEOUT)) {
                try {
                    applyResult(record);
                } catch (IOException e) {
                    consumer.seek(KafkaUtils.getPartition(record), record.offset());
                    log.warn("Could not delete moderated link", e);
                    break;
                }
                KafkaUtils.commit(consumer, record);
            }
        } catch (KafkaException e) {
            if (running.get()) {
                log.warn("Could not consume moderation results", e);
            }
        }
    }

    private void applyResult(ConsumerRecord<String, byte[]> record) throws IOException {
        try {
            ModerationResultEvent event = EventUtils.readModerated(record.value());
            if (!Objects.equals(record.key(), event.id())) {
                log.warn("Result key does not match link ID");
                return;
            }
            if (!event.passed()) {
                linksDao.delete(event.id());
            }
        } catch (IllegalArgumentException | DateTimeException e) {
            log.warn("Invalid moderation result", e);
        }
    }
}
