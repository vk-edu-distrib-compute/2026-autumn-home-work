package company.vk.edu.distrib.compute.katyadoinikova.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class KafkaModerationService implements ModerationService {
    private static final String CREATED_TOPIC = "links.created";
    private static final String MODERATED_TOPIC = "links.moderated";
    private static final String DEAD_LETTER_TOPIC = "links.created.dlt";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final int SEND_TIMEOUT_SECONDS = 10;

    private final String bootstrapServers;
    private final List<String> bannedWords;
    private final Lock lifecycleLock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread worker;
    private KafkaConsumer<String, String> consumer;

    KafkaModerationService(String bootstrapServers) throws IOException {
        this.bootstrapServers = bootstrapServers;
        bannedWords = ModerationSettings.bannedWords();
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            if (worker != null) {
                throw new IllegalStateException("Moderation service has already started");
            }
            consumer = new KafkaConsumer<>(ModerationSettings.consumer(bootstrapServers));
            consumer.subscribe(List.of(CREATED_TOPIC));
            running.set(true);
            worker = Thread.ofVirtual().name("link-moderation").start(this::consume);
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            if (worker == null) {
                return;
            }
            running.set(false);
            consumer.wakeup();
            try {
                worker.join(Duration.ofSeconds(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void consume() {
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(ModerationSettings.producer(bootstrapServers));
                KafkaConsumer<String, String> activeConsumer = consumer) {
            while (running.get()) {
                try {
                    for (ConsumerRecord<String, String> record : activeConsumer.poll(POLL_TIMEOUT)) {
                        process(record, producer);
                        activeConsumer.commitSync();
                    }
                } catch (WakeupException e) {
                    if (running.get()) {
                        throw e;
                    }
                }
            }
        }
    }

    private void process(ConsumerRecord<String, String> record, KafkaProducer<String, String> producer) {
        ModerationResultEvent result;
        try {
            result = moderate(createdEvent(record));
        } catch (RuntimeException e) {
            ProducerRecord<String, String> deadLetter =
                    new ProducerRecord<>(DEAD_LETTER_TOPIC, record.key(), record.value());
            record.headers().forEach(deadLetter.headers()::add);
            deadLetter.headers().add("error", errorMessage(e).getBytes(StandardCharsets.UTF_8));
            send(producer, deadLetter);
            return;
        }
        send(producer, new ProducerRecord<>(MODERATED_TOPIC, result.id(), EventCodec.encodeResult(result)));
    }

    private static LinkCreatedEvent createdEvent(ConsumerRecord<String, String> record) {
        LinkCreatedEvent created = EventCodec.decodeCreated(record.value());
        if (!created.id().equals(record.key())) {
            throw new IllegalArgumentException("Event id does not match its Kafka key");
        }
        return created;
    }

    private ModerationResultEvent moderate(LinkCreatedEvent event) {
        String normalized = URLDecoder.decode(event.url(), StandardCharsets.UTF_8)
                .toLowerCase(Locale.ROOT);
        for (String word : bannedWords) {
            if (normalized.contains(word)) {
                return new ModerationResultEvent(event.id(), false,
                        "URL contains " + word, Instant.now());
            }
        }
        return new ModerationResultEvent(event.id(), true, "", Instant.now());
    }

    private static String errorMessage(RuntimeException error) {
        String message = error.getMessage();
        return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
    }

    private static void send(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Moderation interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Unable to publish moderation result", e);
        }
    }

}
