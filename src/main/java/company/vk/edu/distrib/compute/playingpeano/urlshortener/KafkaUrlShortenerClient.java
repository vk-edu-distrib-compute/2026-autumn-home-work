package company.vk.edu.distrib.compute.playingpeano.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaUrlShortenerClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUrlShortenerClient.class);
    private static final String LINKS_CREATED_TOPIC = "links.created";
    private static final String LINKS_MODERATED_TOPIC = "links.moderated";
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(2);
    private static final int QUEUE_CAPACITY = 1024;
    private static final int EVENTS_PER_POLL = 32;

    private final String bootstrapServers;
    private final Dao<String> links;
    private final Lock linksLock;
    private final BlockingQueue<ProducerRecord<String, String>> createdEvents =
        new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread worker;
    private final AtomicBoolean running = new AtomicBoolean(true);

    KafkaUrlShortenerClient(String bootstrapServers, Dao<String> links, Lock linksLock) {
        this.bootstrapServers = bootstrapServers;
        this.links = links;
        this.linksLock = linksLock;
        worker = Thread.ofPlatform().daemon().name("playingpeano-url-shortener-kafka").unstarted(this::run);
    }

    void start() {
        worker.start();
    }

    void stop() {
        running.set(false);
        worker.interrupt();
        try {
            worker.join(STOP_TIMEOUT);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    void publishCreated(LinkCreatedEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
            LINKS_CREATED_TOPIC,
            event.id(),
            ModerationEventCodec.encodeCreated(event)
        );
        if (!createdEvents.offer(record) && LOGGER.isWarnEnabled()) {
            LOGGER.warn("Moderation queue is full for link {}", event.id());
        }
    }

    private void run() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(LINKS_MODERATED_TOPIC));
            while (running.get()) {
                publishQueuedEvents(producer);
                for (ConsumerRecord<String, String> record : consumer.poll(POLL_INTERVAL)) {
                    applyAndCommit(consumer, record);
                }
            }
        } catch (KafkaException exception) {
            if (running.get()) {
                LOGGER.warn("URL shortener Kafka worker stopped", exception);
            }
        }
    }

    private void publishQueuedEvents(KafkaProducer<String, String> producer) {
        int batchSize = Math.min(createdEvents.size(), EVENTS_PER_POLL);
        for (int index = 0; index < batchSize && running.get(); index++) {
            @Nullable ProducerRecord<String, String> record = createdEvents.poll();
            if (record == null) {
                return;
            }
            producer.send(record, (metadata, exception) -> handlePublishResult(record, exception));
        }
    }

    private void handlePublishResult(ProducerRecord<String, String> record, @Nullable Exception exception) {
        if (exception == null || !running.get()) {
            return;
        }
        if (exception instanceof RetriableException && createdEvents.offer(record)) {
            return;
        }
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("Cannot publish moderation event for link {}", record.key(), exception);
        }
    }

    private void applyAndCommit(KafkaConsumer<String, String> consumer, ConsumerRecord<String, String> record) {
        @Nullable String value = record.value();
        try {
            if (value != null) {
                applyResult(ModerationEventCodec.decodeResult(value), record.key());
            }
        } catch (IllegalArgumentException exception) {
            LOGGER.warn("Ignoring malformed moderation result", exception);
        } catch (IOException exception) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Cannot remove rejected link {}", record.key(), exception);
            }
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            return;
        }
        consumer.commitAsync();
    }

    private void applyResult(ModerationEventCodec.Result result, @Nullable String key) throws IOException {
        if (result.passed() || !result.id().equals(key)) {
            return;
        }

        HttpUtils.validateId(result.id());
        linksLock.lock();
        try {
            if (result.originalUrl().equals(links.get(result.id()))) {
                links.delete(result.id());
            }
        } catch (NoSuchElementException expected) {
        } finally {
            linksLock.unlock();
        }
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 250);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        return properties;
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "playingpeano-url-shortener-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}
