package company.vk.edu.distrib.compute.k0oshara.urlshortener;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

final class KafkaModerationService implements ModerationService {
    private static final System.Logger LOGGER = System.getLogger(KafkaModerationService.class.getName());
    private static final String GROUP_ID = "k0oshara-url-moderation";
    private static final String ERROR_HEADER = "error";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(250);
    private static final Duration OUTPUT_TIMEOUT = Duration.ofSeconds(5);
    private static final long STOP_TIMEOUT_MILLIS = 65_000;
    private final String bootstrapServers;
    private final List<String> bannedWords;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final AtomicReference<KafkaConsumer<byte[], byte[]>> consumer = new AtomicReference<>();
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private Thread worker;

    KafkaModerationService(String servers, List<String> words) {
        bootstrapServers = servers;
        bannedWords = List.copyOf(words);
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            if (worker != null) {
                throw new IllegalStateException("Moderation service was already started");
            }
            worker = Thread.ofPlatform().name("kafka-link-moderation").unstarted(this::run);
            worker.start();
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        Thread currentWorker;
        lifecycleLock.lock();
        try {
            if (worker == null) {
                return;
            }
            currentWorker = worker;
            stopping.set(true);
        } finally {
            lifecycleLock.unlock();
        }
        Optional.ofNullable(consumer.get()).ifPresent(KafkaConsumer::wakeup);
        try {
            currentWorker.join(STOP_TIMEOUT_MILLIS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping moderation service", exception);
        }
        if (currentWorker.isAlive()) {
            throw new IllegalStateException("Moderation service did not stop within the timeout");
        }
    }

    private void run() {
        try {
            if (stopping.get()) {
                return;
            }
            try (KafkaConsumer<byte[], byte[]> input = ModerationKafka.consumer(bootstrapServers, GROUP_ID);
                 KafkaProducer<byte[], byte[]> output = ModerationKafka.producer(bootstrapServers)) {
                consumer.set(input);
                consume(input, output);
            }
        } catch (WakeupException exception) {
            if (!stopping.get()) {
                logFailure("Kafka consumer was woken unexpectedly", exception);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            if (!stopping.get()) {
                logFailure("Kafka moderation worker was interrupted", exception);
            }
        } catch (Exception exception) {
            logFailure("Kafka moderation worker failed", exception);
        } finally {
            consumer.set(null);
        }
    }

    private void consume(KafkaConsumer<byte[], byte[]> input, KafkaProducer<byte[], byte[]> output)
        throws ExecutionException, InterruptedException, TimeoutException {
        input.subscribe(List.of(ModerationKafka.CREATED));
        while (!stopping.get()) {
            for (ConsumerRecord<byte[], byte[]> record : input.poll(POLL_TIMEOUT)) {
                if (stopping.get()) {
                    return;
                }
                output.send(outputRecord(record)).get(OUTPUT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                commit(record, input);
            }
        }
    }

    private ProducerRecord<byte[], byte[]> outputRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            LinkCreatedEvent event = ModerationKafka.decodeCreated(record.key(), record.value());
            return new ProducerRecord<>(ModerationKafka.MODERATED, record.key(),
                ModerationKafka.encode(moderate(event)));
        } catch (IllegalArgumentException exception) {
            return deadLetter(record, exception);
        }
    }

    private ModerationResultEvent moderate(LinkCreatedEvent event) {
        URI uri = URI.create(event.url());
        if (!uri.isAbsolute() || uri.getHost() == null) {
            throw new IllegalArgumentException("Malformed URL");
        }
        String decoded = URLDecoder.decode(event.url().replace("+", "%2B"), StandardCharsets.UTF_8)
            .toLowerCase(Locale.ROOT);
        String bannedWord = bannedWords.stream().filter(decoded::contains).findFirst().orElse(null);
        boolean passed = bannedWord == null;
        String reason = passed ? null : "Contains banned word: " + bannedWord;
        return new ModerationResultEvent(event.id(), passed, reason, Instant.now());
    }

    private static ProducerRecord<byte[], byte[]> deadLetter(
        ConsumerRecord<byte[], byte[]> record,
        IllegalArgumentException exception
    ) {
        Headers headers = new RecordHeaders(record.headers());
        String message = exception.getMessage();
        String error = message == null || message.isBlank() ? exception.getClass().getSimpleName() : message;
        headers.add(ERROR_HEADER, error.getBytes(StandardCharsets.UTF_8));
        Long timestamp = record.timestamp() < 0 ? null : record.timestamp();
        return new ProducerRecord<>(ModerationKafka.DEAD_LETTER, null, timestamp,
            record.key(), record.value(), headers);
    }

    private static void commit(ConsumerRecord<byte[], byte[]> record, KafkaConsumer<byte[], byte[]> input) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        try {
            input.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)), OUTPUT_TIMEOUT);
        } catch (CommitFailedException | RebalanceInProgressException exception) {
            LOGGER.log(System.Logger.Level.WARNING, "Kafka rebalance delayed an offset commit", exception);
            if (input.assignment().contains(partition)) {
                try {
                    input.seek(partition, record.offset());
                } catch (IllegalStateException seekException) {
                    LOGGER.log(System.Logger.Level.WARNING, "Kafka partition was revoked before retry seek",
                        seekException);
                }
            }
        }
    }

    private static void logFailure(String message, Exception exception) {
        LOGGER.log(System.Logger.Level.ERROR, message, exception);
    }
}
