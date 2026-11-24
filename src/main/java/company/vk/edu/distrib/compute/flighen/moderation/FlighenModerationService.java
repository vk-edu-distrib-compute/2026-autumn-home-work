package company.vk.edu.distrib.compute.flighen.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class FlighenModerationService implements ModerationService {
    private static final Logger log = LoggerFactory.getLogger(FlighenModerationService.class);
    private static final String CREATED_TOPIC = "links.created";
    private static final String MODERATED_TOPIC = "links.moderated";
    private static final String DEAD_LETTER_TOPIC = "links.created.dlt";
    private static final String ERROR_HEADER = "error";
    private static final String GROUP_ID = "flighen-moderation";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(500);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(1);
    private static final int STOP_TIMEOUT_MS = 5000;

    private final ModerationEventCodec codec = new ModerationEventCodec();
    private final List<String> bannedWords;
    private final KafkaConsumer<String, byte[]> consumer;
    private final KafkaProducer<String, byte[]> producer;
    private final Thread worker = new Thread(this::consume, "flighen-moderation");

    private final AtomicBoolean running = new AtomicBoolean();
    private boolean started;
    private boolean stopped;

    public FlighenModerationService(String bootstrapServers) throws IOException {
        bannedWords = loadBannedWords();
        consumer = createConsumer(bootstrapServers);
        try {
            producer = createProducer(bootstrapServers);
        } catch (RuntimeException e) {
            consumer.close(CLOSE_TIMEOUT);
            throw e;
        }
    }

    @Override
    public void start() {
        if (started || stopped) {
            throw new IllegalStateException();
        }
        running.set(true);
        started = true;
        worker.start();
    }

    @Override
    public void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        running.set(false);
        if (!started) {
            closeClients();
            return;
        }

        consumer.wakeup();
        try {
            worker.join(STOP_TIMEOUT_MS);
            if (worker.isAlive()) {
                worker.interrupt();
            }
        } catch (InterruptedException e) {
            worker.interrupt();
            Thread.currentThread().interrupt();
        }
    }

    private void consume() {
        try {
            consumer.subscribe(List.of(CREATED_TOPIC));
            while (running.get()) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(POLL_TIMEOUT)) {
                    processRecord(record);
                }
            }
        } catch (WakeupException e) {
            if (running.get() && log.isErrorEnabled()) {
                log.error("Moderation consumer was unexpectedly interrupted", e);
            }
        } catch (KafkaException e) {
            if (log.isErrorEnabled()) {
                log.error("Moderation consumer failed", e);
            }
        } finally {
            closeClients();
        }
    }

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        try {
            moderate(record);
        } catch (InterruptedException e) {
            running.set(false);
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException | KafkaException e) {
            if (running.get()) {
                if (log.isWarnEnabled()) {
                    log.warn("Could not process moderation record for {}", record.key(), e);
                }
                consumer.seek(partition, record.offset());
            }
            return;
        }

        try {
            consumer.commitSync(COMMIT_TIMEOUT);
        } catch (KafkaException e) {
            if (running.get() && log.isWarnEnabled()) {
                log.warn("Could not commit moderation offset", e);
            }
        }
    }

    private void moderate(ConsumerRecord<String, byte[]> record)
            throws InterruptedException, ExecutionException {
        try {
            LinkCreatedEvent event = codec.deserializeLinkCreated(record.value());
            String url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
            String reason = "";
            for (String word : bannedWords) {
                if (url.contains(word)) {
                    reason = "URL contains banned word: " + word;
                    break;
                }
            }

            ModerationResultEvent result = new ModerationResultEvent(
                    event.id(), reason.isEmpty(), reason, Instant.now()
            );
            producer.send(new ProducerRecord<>(
                    MODERATED_TOPIC, event.id(), codec.serializeModerationResult(result)
            )).get();
        } catch (IOException | IllegalArgumentException e) {
            sendToDeadLetter(record, e.toString());
        }
    }

    private void sendToDeadLetter(ConsumerRecord<String, byte[]> record, String error)
            throws InterruptedException, ExecutionException {
        ProducerRecord<String, byte[]> deadLetter = new ProducerRecord<>(
                DEAD_LETTER_TOPIC, record.key(), record.value()
        );
        record.headers().forEach(header -> deadLetter.headers().add(header));
        deadLetter.headers().add(ERROR_HEADER, error.getBytes(StandardCharsets.UTF_8));
        producer.send(deadLetter).get();
    }

    private void closeClients() {
        try {
            consumer.close(CLOSE_TIMEOUT);
        } catch (KafkaException e) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to close moderation consumer", e);
            }
        }
        try {
            producer.close(CLOSE_TIMEOUT);
        } catch (KafkaException e) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to close moderation producer", e);
            }
        }
    }

    private static List<String> loadBannedWords() throws IOException {
        try (InputStream stream = FlighenModerationService.class.getResourceAsStream("/moderation/banned_words.txt")) {
            if (stream == null) {
                throw new IOException("Banned words resource is missing");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                return reader.lines()
                        .map(word -> word.trim().toLowerCase(Locale.ROOT))
                        .filter(word -> !word.isEmpty())
                        .toList();
            }
        }
    }

    private static KafkaConsumer<String, byte[]> createConsumer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        return new KafkaConsumer<>(properties);
    }

    private static KafkaProducer<String, byte[]> createProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
