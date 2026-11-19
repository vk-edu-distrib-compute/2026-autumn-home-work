package company.vk.edu.distrib.compute.fedorkhokhryakov.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class KafkaModerationService implements ModerationService {
    private static final Logger log = LoggerFactory.getLogger(KafkaModerationService.class);
    private static final String GROUP_ID = "fedorkhokhryakov-moderation";
    private static final String PASSED_REASON = "ok";
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);

    private final String bootstrapServers;
    private final BannedWords bannedWords;
    private final Lock lock = new ReentrantLock();
    private Optional<Running> running = Optional.empty();

    public KafkaModerationService(String bootstrapServers, BannedWords bannedWords) {
        this.bootstrapServers = bootstrapServers;
        this.bannedWords = bannedWords;
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (running.isPresent()) {
                throw new IllegalStateException("Moderation service has already been started");
            }
            Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig());
            PollingWorker<byte[], byte[]> worker = new PollingWorker<>(
                "moderation-worker",
                new KafkaConsumer<>(consumerConfig()),
                ModerationTopics.LINKS_CREATED,
                records -> moderate(producer, records),
                true
            );
            worker.start();
            running = Optional.of(new Running(producer, worker));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            Running current = running.orElseThrow(
                () -> new IllegalStateException("Moderation service has not been started"));
            current.worker().shutdown();
            current.producer().close(CLOSE_TIMEOUT);
            running = Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    private void moderate(Producer<byte[], byte[]> producer, ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            process(producer, record);
        }
        producer.flush();
    }

    private void process(Producer<byte[], byte[]> producer, ConsumerRecord<byte[], byte[]> record) {
        LinkCreatedEvent event;
        try {
            event = EventCodecs.decodeLinkCreated(record.value());
        } catch (IllegalArgumentException e) {
            sendToDeadLetter(producer, record, e);
            return;
        }
        ModerationResultEvent result = check(event);
        producer.send(
            new ProducerRecord<>(
                ModerationTopics.LINKS_MODERATED,
                event.id().getBytes(StandardCharsets.UTF_8),
                EventCodecs.encode(result)
            ),
            (metadata, error) -> {
                if (error != null) {
                    log.warn("Failed to publish moderation result of {}", event.id(), error);
                }
            }
        );
    }

    private ModerationResultEvent check(LinkCreatedEvent event) {
        Optional<String> bannedWord = bannedWords.findIn(event.url());
        String reason = bannedWord.map(word -> "contains banned word: " + word).orElse(PASSED_REASON);
        return new ModerationResultEvent(event.id(), bannedWord.isEmpty(), reason, Instant.now());
    }

    private static void sendToDeadLetter(
        Producer<byte[], byte[]> producer,
        ConsumerRecord<byte[], byte[]> record,
        Exception cause
    ) {
        String message = cause.getMessage();
        String error = message == null || message.isBlank() ? cause.getClass().getName() : message;
        RecordHeaders headers = new RecordHeaders(record.headers().toArray());
        headers.add(ModerationTopics.ERROR_HEADER, error.getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(new ProducerRecord<>(
                ModerationTopics.LINKS_CREATED_DLT,
                null,
                record.key(),
                record.value(),
                headers
            ));
        } catch (KafkaException e) {
            log.warn("Failed to send message at offset {} to dead letter topic", record.offset(), e);
        }
        log.info("Message at {}-{} offset {} sent to dead letter topic: {}",
            record.topic(), record.partition(), record.offset(), error);
    }

    private Map<String, Object> consumerConfig() {
        return Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        );
    }

    private Map<String, Object> producerConfig() {
        return Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );
    }

    private record Running(Producer<byte[], byte[]> producer, PollingWorker<byte[], byte[]> worker) {
    }
}
