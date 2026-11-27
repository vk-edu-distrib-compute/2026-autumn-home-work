package company.vk.edu.distrib.compute.robert.moderation;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RobertModerationService implements ModerationService {
    private static final Logger log = LoggerFactory.getLogger(RobertModerationService.class);

    private static final String LINKS_CREATED = "links.created";
    private static final String LINKS_MODERATED = "links.moderated";
    private static final String LINKS_CREATED_DLT = "links.created.dlt";
    private static final String ERROR_HEADER = "error";

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final long RETRY_DELAY_MILLIS = 200;

    private final List<String> bannedWords;
    private final KafkaProducer<String, byte[]> producer;
    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread worker;

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean();

    public RobertModerationService(String inputBootstrapServers) throws IOException {
        bannedWords = loadBannedWords();

        consumer = new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, inputBootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "robert-moderation",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic"
            ),
            new StringDeserializer(),
            new ByteArrayDeserializer()
        );

        producer = new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, inputBootstrapServers,
                ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576L,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000L,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
            ),
            new StringSerializer(),
            new ByteArraySerializer()
        );

        worker = new Thread(this::consumeLinks, "robert-moderation");
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (worker.getState() != Thread.State.NEW) {
                throw new IllegalStateException();
            }

            running.set(true);
            worker.start();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (!running.get()) {
                throw new IllegalStateException();
            }

            running.set(false);
            consumer.wakeup();
            producer.close(Duration.ZERO);

            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            lock.unlock();
        }
    }

    // чтобы явно вызвать consumer.close(CLOSE_TIMEOUT); в finally
    @SuppressWarnings("PMD.UseTryWithResources")
    private void consumeLinks() {
        try {
            consumer.subscribe(List.of(LINKS_CREATED));

            while (running.get()) {
                try {
                    for (ConsumerRecord<String, byte[]> record : consumer.poll(POLL_TIMEOUT)) {
                        sendResult(moderate(record));

                        if (running.get()) {
                            consumer.commitSync();
                        }
                    }
                } catch (WakeupException e) {
                    if (running.get()) {
                        throw e;
                    }
                } catch (KafkaException e) {
                    log.atWarn().setCause(e).log("Could not consume links");
                    Thread.sleep(RETRY_DELAY_MILLIS);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.close(CLOSE_TIMEOUT);
        }
    }

    private ProducerRecord<String, byte[]> moderate(ConsumerRecord<String, byte[]> record) {
        try {
            LinkCreatedEvent event = decodeCreated(record);
            String url = URLDecoder.decode(
                event.url().replace("+", "%2B"),
                StandardCharsets.UTF_8
            ).toLowerCase(Locale.ROOT);

            String reason = bannedWords.stream().filter(url::contains).findFirst().orElse("");
            ModerationResultEvent result = new ModerationResultEvent(
                event.id(),
                reason.isEmpty(),
                reason,
                Instant.now()
            );

            return new ProducerRecord<>(
                LINKS_MODERATED,
                event.id(),
                ModerationEventUtils.encodeResult(result).getBytes(StandardCharsets.UTF_8)
            );
        } catch (RuntimeException e) {
            RecordHeaders headers = new RecordHeaders(record.headers().toArray());
            headers.add(ERROR_HEADER, e.toString().getBytes(StandardCharsets.UTF_8));

            return new ProducerRecord<>(
                LINKS_CREATED_DLT,
                null,
                record.key(),
                record.value(),
                headers
            );
        }
    }

    private static LinkCreatedEvent decodeCreated(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            throw new IllegalArgumentException();
        }

        LinkCreatedEvent event = ModerationEventUtils.decodeCreated(
            new String(record.value(), StandardCharsets.UTF_8)
        );

        if (event.id().isEmpty() || !event.id().equals(record.key()) || event.url().isEmpty()) {
            throw new IllegalArgumentException();
        }

        return event;
    }

    private void sendResult(ProducerRecord<String, byte[]> record) throws InterruptedException {
        while (running.get()) {
            try {
                producer.send(record).get();
                return;
            } catch (ExecutionException | KafkaException e) {
                if (running.get()) {
                    log.atWarn().setCause(e).log("Could not send moderation result");
                    Thread.sleep(RETRY_DELAY_MILLIS);
                }
            }
        }
    }

    private static List<String> loadBannedWords() throws IOException {
        var stream = RobertModerationService.class.getResourceAsStream("/moderation/banned_words.txt");
        if (stream == null) {
            throw new IOException();
        }

        try (stream) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8).lines()
                .map(String::strip)
                .filter(word -> !word.isEmpty())
                .map(word -> word.toLowerCase(Locale.ROOT))
                .toList();
        }
    }
}
