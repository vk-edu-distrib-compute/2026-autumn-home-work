package company.vk.edu.distrib.compute.virogg.urlshortener.links;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaModerationService implements ModerationService {
    private static final String GROUP = "virogg-moderation";
    private static final Logger log = LoggerFactory.getLogger(KafkaModerationService.class);

    private final String bootstrapServers;
    private final List<String> bannedWords;
    private final Lock lifecycle = new ReentrantLock();
    @Nullable
    private ExecutorService worker;

    KafkaModerationService(String bootstrapServers, List<String> bannedWords) {
        this.bootstrapServers = bootstrapServers;
        this.bannedWords = bannedWords;
    }

    @Override
    public void start() {
        lifecycle.lock();
        try {
            if (worker != null && !worker.isTerminated()) {
                throw new IllegalStateException("Moderation service is already running");
            }
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            worker = executor;
            executor.execute(this::moderate);
        } finally {
            lifecycle.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycle.lock();
        try {
            if (worker != null && !worker.isTerminated()) {
                worker.shutdownNow();
                worker.close();
            }
        } finally {
            lifecycle.unlock();
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources") // Kafka default producer close has no deadline.
    private void moderate() {
        try {
            KafkaProducer<byte[], byte[]> producer = ModerationKafka.producer(bootstrapServers);
            try {
                ModerationKafka.consume(bootstrapServers, GROUP, ModerationKafka.CREATED,
                        record -> producer.send(result(record)).get());
            } finally {
                ModerationKafka.close(producer);
            }
        } catch (InterruptException expected) {
            Thread.currentThread().interrupt();
        } catch (KafkaException e) {
            log.warn("Moderation service stopped", e);
        }
    }

    private ProducerRecord<byte[], byte[]> result(ConsumerRecord<byte[], byte[]> record) {
        try {
            LinkCreatedEvent event = ModerationKafka.readCreated(record);
            String url = URLDecoder.decode(event.url().replace("+", "%2B"), StandardCharsets.UTF_8)
                    .toLowerCase(Locale.ROOT);
            boolean passed = bannedWords.stream().noneMatch(url::contains);
            String reason = passed ? "" : "URL contains a banned word";
            return ModerationKafka.moderated(new ModerationResultEvent(event.id(), passed, reason, Instant.now()));
        } catch (IllegalArgumentException | DateTimeException e) {
            ProducerRecord<byte[], byte[]> deadLetter =
                    new ProducerRecord<>(ModerationKafka.DLT, null, record.key(), record.value(), record.headers());
            deadLetter.headers().add("error", ModerationKafka.bytes("Invalid link creation event: " + e.getMessage()));
            return deadLetter;
        }
    }
}
