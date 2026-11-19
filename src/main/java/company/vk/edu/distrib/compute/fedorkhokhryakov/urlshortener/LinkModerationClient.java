package company.vk.edu.distrib.compute.fedorkhokhryakov.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.fedorkhokhryakov.moderation.EventCodecs;
import company.vk.edu.distrib.compute.fedorkhokhryakov.moderation.ModerationTopics;
import company.vk.edu.distrib.compute.fedorkhokhryakov.moderation.PollingWorker;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

public final class LinkModerationClient implements LinkCreatedListener {
    private static final Logger log = LoggerFactory.getLogger(LinkModerationClient.class);
    private static final String GROUP_ID = "fedorkhokhryakov-url-shortener";
    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(500);
    private static final int MAX_BLOCK_MS = 5000;
    private static final int RECONNECT_BACKOFF_MAX_MS = 5000;

    private final Producer<byte[], byte[]> producer;
    private final ExecutorService sender;
    private final PollingWorker<byte[], byte[]> results;
    private final Dao<String> dao;

    private LinkModerationClient(String bootstrapServers, Dao<String> dao) {
        this.dao = dao;
        this.producer = new KafkaProducer<>(producerConfig(bootstrapServers));
        this.sender = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().name("link-created-sender").daemon().factory());
        this.results = new PollingWorker<>(
            "moderation-results-listener",
            new KafkaConsumer<>(consumerConfig(bootstrapServers)),
            ModerationTopics.LINKS_MODERATED,
            this::applyResults,
            false
        );
    }

    public static Optional<LinkModerationClient> start(String bootstrapServers, Dao<String> dao) {
        try {
            LinkModerationClient client = new LinkModerationClient(bootstrapServers, dao);
            client.results.start();
            return Optional.of(client);
        } catch (KafkaException e) {
            log.warn("Link moderation is disabled: failed to connect to Kafka {}", bootstrapServers, e);
            return Optional.empty();
        }
    }

    @Override
    public void linkCreated(String id, String url) {
        LinkCreatedEvent event = new LinkCreatedEvent(id, url, Instant.now());
        try {
            sender.execute(() -> publish(event));
        } catch (RejectedExecutionException e) {
            log.warn("Link {} is not sent to moderation: client is closed", id, e);
        }
    }

    public void close() {
        sender.shutdownNow();
        results.shutdown();
        try {
            producer.close(CLOSE_TIMEOUT);
        } catch (KafkaException e) {
            log.warn("Failed to close Kafka producer", e);
        }
    }

    private void publish(LinkCreatedEvent event) {
        try {
            producer.send(
                new ProducerRecord<>(
                    ModerationTopics.LINKS_CREATED,
                    event.id().getBytes(StandardCharsets.UTF_8),
                    EventCodecs.encode(event)
                ),
                (metadata, error) -> {
                    if (error != null && log.isWarnEnabled()) {
                        log.warn("Failed to send link {} to moderation", event.id(), error);
                    }
                }
            );
        } catch (KafkaException e) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to send link {} to moderation", event.id(), e);
            }
        }
    }

    private void applyResults(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            try {
                apply(EventCodecs.decodeModerationResult(record.value()));
            } catch (IllegalArgumentException e) {
                if (log.isWarnEnabled()) {
                    log.warn("Skipping malformed moderation result at offset {}", record.offset(), e);
                }
            }
        }
    }

    private void apply(ModerationResultEvent result) {
        if (result.passed()) {
            return;
        }
        try {
            dao.delete(result.id());
            if (log.isInfoEnabled()) {
                log.info("Link {} removed by moderation: {}", result.id(), result.reason());
            }
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to remove link {}", result.id(), e);
            }
        }
    }

    private static Map<String, Object> producerConfig(String bootstrapServers) {
        return Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS,
            ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, RECONNECT_BACKOFF_MAX_MS,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );
    }

    private static Map<String, Object> consumerConfig(String bootstrapServers) {
        return Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, RECONNECT_BACKOFF_MAX_MS,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        );
    }
}
