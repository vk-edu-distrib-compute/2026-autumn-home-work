package company.vk.edu.distrib.compute.rsmt98.urlshortener;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.rsmt98.moderation.ModerationEventUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class KafkaLinkModeration implements AutoCloseable {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration RETRY_DELAY = Duration.ofMillis(500);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private final String bootstrapServers;
    private final LinkStore links;
    private final BlockingQueue<LinkCreatedEvent> pendingEvents = new LinkedBlockingQueue<>();
    private final Set<String> pendingDeletes = new HashSet<>();
    private final Thread receiver;
    private final Thread publisher;
    private final AtomicBoolean running = new AtomicBoolean(true);

    KafkaLinkModeration(String bootstrapServers, LinkStore links) {
        this.bootstrapServers = bootstrapServers;
        this.links = links;
        receiver = Thread.ofVirtual().unstarted(this::runReceiver);
        publisher = Thread.ofVirtual().unstarted(this::runPublisher);
    }

    void start() {
        receiver.start();
        publisher.start();
    }

    void created(String id, String url) {
        if (running.get()) {
            pendingEvents.offer(new LinkCreatedEvent(id, url, Instant.now()));
        }
    }

    private void runReceiver() {
        KafkaConsumer<String, byte[]> consumer = null;
        try {
            consumer = createConsumer();
            consumer.subscribe(List.of(ModerationEventUtils.MODERATED_TOPIC));
            while (running.get()) {
                try {
                    pollAndApply(consumer);
                } catch (RuntimeException expected) {
                }
            }
        } finally {
            Thread.interrupted();
            closeConsumer(consumer);
        }
    }

    private void runPublisher() {
        KafkaProducer<String, byte[]> producer = null;
        try {
            producer = createProducer();
            while (running.get()) {
                publish(producer, pendingEvents.take());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            Thread.interrupted();
            closeProducer(producer);
        }
    }

    private void publish(KafkaProducer<String, byte[]> producer, LinkCreatedEvent event)
            throws InterruptedException {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(
                        ModerationEventUtils.CREATED_TOPIC,
                        event.id(),
                        ModerationEventUtils.serializeCreated(event));
        while (running.get()) {
            try {
                producer.send(record).get();
                return;
            } catch (ExecutionException | RuntimeException e) {
                Thread.sleep(RETRY_DELAY);
            }
        }
    }

    private KafkaProducer<String, byte[]> createProducer() {
        return new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        bootstrapServers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class));
    }

    private KafkaConsumer<String, byte[]> createConsumer() {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,
                        UUID.randomUUID().toString(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",
                        ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                        false,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        ByteArrayDeserializer.class));
    }

    private void pollAndApply(KafkaConsumer<String, byte[]> consumer) {
        for (ConsumerRecord<String, byte[]> record : consumer.poll(POLL_TIMEOUT)) {
            if (!running.get()) {
                break;
            }
            apply(record);
        }
        processPendingDeletes();
    }

    private static void closeConsumer(@Nullable KafkaConsumer<String, byte[]> consumer) {
        try {
            if (consumer != null) {
                consumer.close(CLOSE_TIMEOUT);
            }
        } catch (RuntimeException expected) {
        }
    }

    private static void closeProducer(@Nullable KafkaProducer<String, byte[]> producer) {
        try {
            if (producer != null) {
                producer.close(CLOSE_TIMEOUT);
            }
        } catch (RuntimeException expected) {
        }
    }

    private void apply(ConsumerRecord<String, byte[]> record) {
        try {
            if (record.value() == null) {
                return;
            }
            ModerationResultEvent result = ModerationEventUtils.deserializeResult(record.value());
            if (!result.id().equals(record.key())) {
                return;
            }
            if (Boolean.FALSE.equals(result.passed())) {
                LinkStore.validateId(result.id());
                pendingDeletes.add(result.id());
            }
        } catch (IllegalArgumentException expected) {
        }
    }

    private void processPendingDeletes() {
        Iterator<String> ids = pendingDeletes.iterator();
        while (running.get() && ids.hasNext()) {
            try {
                links.delete(ids.next());
                ids.remove();
            } catch (IOException expected) {
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        receiver.interrupt();
        publisher.interrupt();
        boolean interrupted = false;
        while (receiver.isAlive() || publisher.isAlive()) {
            try {
                receiver.join();
                publisher.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        pendingEvents.clear();
        pendingDeletes.clear();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
