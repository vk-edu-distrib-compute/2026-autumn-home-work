package company.vk.edu.distrib.compute.virogg.urlshortener.links;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ModerationKafka {
    static final String CREATED = "links.created";
    static final String MODERATED = "links.moderated";
    static final String DLT = "links.created.dlt";
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final Logger log = LoggerFactory.getLogger(ModerationKafka.class);

    private ModerationKafka() {
    }

    static KafkaProducer<byte[], byte[]> producer(String bootstrapServers) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000
        ), new ByteArraySerializer(), new ByteArraySerializer());
    }

    static void close(KafkaProducer<byte[], byte[]> producer) {
        Thread.interrupted();
        producer.close(CLOSE_TIMEOUT);
    }

    @SuppressWarnings("PMD.UseTryWithResources") // shutdown even when the Kafka group coordinator is down
    static void consume(String bootstrapServers, String group, String topic, RecordHandler handler) {
        try {
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                    ConsumerConfig.GROUP_ID_CONFIG, group,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                    ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1
            ), new ByteArrayDeserializer(), new ByteArrayDeserializer());
            try {
                consumer.subscribe(List.of(topic));
                while (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL_INTERVAL)) {
                        handle(consumer, record, handler);
                    }
                }
            } finally {
                Thread.interrupted();
                consumer.close(CloseOptions.timeout(CLOSE_TIMEOUT));
            }
        } catch (InterruptedException | InterruptException expected) {
            Thread.currentThread().interrupt();
        } catch (KafkaException e) {
            log.warn("Kafka consumer for {} stopped", topic, e);
        }
    }

    private static void handle(KafkaConsumer<byte[], byte[]> consumer, ConsumerRecord<byte[], byte[]> record,
                               RecordHandler handler) throws InterruptedException {
        try {
            handler.process(record);
        } catch (InterruptException expected) {
            Thread.currentThread().interrupt();
            return;
        } catch (IOException | ExecutionException | KafkaException e) {
            if (log.isWarnEnabled()) {
                log.warn("Could not process {} at offset {}", record.topic(), record.offset(), e);
            }
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            Thread.sleep(POLL_INTERVAL);
            return;
        }
        try {
            consumer.commitSync();
        } catch (InterruptException expected) {
            Thread.currentThread().interrupt();
        } catch (KafkaException e) {
            if (log.isWarnEnabled()) {
                log.warn("Could not commit offset for {}", record.topic(), e);
            }
        }
    }

    static ProducerRecord<byte[], byte[]> created(LinkCreatedEvent event) {
        return new ProducerRecord<>(CREATED, bytes(event.id()), bytes(event.createdAt() + "\n" + event.url()));
    }

    static LinkCreatedEvent readCreated(ConsumerRecord<byte[], byte[]> record) {
        String[] fields = decode(record.value()).split("\n", 2);
        if (fields.length != 2 || LinkUrlUtils.toAsciiUri(fields[1]) == null) {
            throw new IllegalArgumentException("Expected a timestamp and a valid URL");
        }
        return new LinkCreatedEvent(id(record), fields[1], Instant.parse(fields[0]));
    }

    static ProducerRecord<byte[], byte[]> moderated(ModerationResultEvent event) {
        String value = event.passed() + "\n" + event.moderatedAt() + "\n" + event.reason();
        return new ProducerRecord<>(MODERATED, bytes(event.id()), bytes(value));
    }

    static ModerationResultEvent readModerated(ConsumerRecord<byte[], byte[]> record) {
        String[] fields = decode(record.value()).split("\n", 3);
        if (fields.length != 3 || !("true".equals(fields[0]) || "false".equals(fields[0]))) {
            throw new IllegalArgumentException("Expected a decision, timestamp and reason");
        }
        return new ModerationResultEvent(id(record), Boolean.valueOf(fields[0]), fields[2], Instant.parse(fields[1]));
    }

    static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String id(ConsumerRecord<byte[], byte[]> record) {
        String id = decode(record.key());
        if (id.isBlank()) {
            throw new IllegalArgumentException("Link ID is empty");
        }
        return id;
    }

    private static String decode(byte @Nullable [] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Missing message key or value");
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid UTF-8", e);
        }
    }

    @FunctionalInterface
    interface RecordHandler {
        void process(ConsumerRecord<byte[], byte[]> record)
                throws IOException, ExecutionException, InterruptedException;
    }
}
