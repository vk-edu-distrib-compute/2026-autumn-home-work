package company.vk.edu.distrib.compute.masha533.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ModerationResultConsumer {
    private final Dao<String> linksDao;
    private KafkaConsumer<String, String> moderationConsumer;
    private Thread moderationWorker;
    private final AtomicBoolean moderationRunning = new AtomicBoolean(false);

    public ModerationResultConsumer(Dao<String> linksDao) {
        this.linksDao = linksDao;
    }

    void applyModerationResult(ConsumerRecord<String, String> record) throws IOException {
        final var id = record.key();
        final var value = record.value();
        if (id == null || !id.matches(UrlShortenerServiceImpl.ID_PATTERN) || value == null) {
            return;
        }

        final var parts = value.split("\n", 3);
        if (parts.length == 3 && "false".equals(parts[0])) {
            linksDao.delete(id);
        }
    }

    static KafkaConsumer<String, String> createModerationConsumer(String kafkaBootstrapServers) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "masha533-shortener");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(properties);
    }

    private void consumeModerationResults() {
        try (var consumer = moderationConsumer) {
            consumer.subscribe(List.of("links.moderated"));

            while (moderationRunning.get()) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    applyModerationResult(record);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (moderationRunning.get()) {
                throw e;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to apply moderation result", e);
        }
    }

    void start(String bootstrapServers) {
        moderationConsumer = createModerationConsumer(bootstrapServers);
        moderationRunning.set(true);
        moderationWorker = new Thread(this::consumeModerationResults, "masha533-moderation-results");
        moderationWorker.start();
    }

    void stop() {
        if (moderationWorker == null) {
            return;
        }
        moderationRunning.set(false);
        moderationConsumer.wakeup();
        try {
            moderationWorker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
