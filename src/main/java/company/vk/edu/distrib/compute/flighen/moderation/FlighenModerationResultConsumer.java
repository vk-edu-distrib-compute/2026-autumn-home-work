package company.vk.edu.distrib.compute.flighen.moderation;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public final class FlighenModerationResultConsumer {
    private static final Logger log = LoggerFactory.getLogger(FlighenModerationResultConsumer.class);
    private static final String TOPIC = "links.moderated";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(500);
    private static final int STOP_TIMEOUT_MS = 2000;

    private final Dao<String> dao;
    private final ModerationEventCodec codec = new ModerationEventCodec();
    private final KafkaConsumer<String, byte[]> consumer;
    private final Thread worker = new Thread(this::consume, "flighen-moderation-results");
    private final AtomicBoolean running = new AtomicBoolean();

    public FlighenModerationResultConsumer(String bootstrapServers, Dao<String> dao) {
        this.dao = dao;

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flighen-shortener-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        consumer = new KafkaConsumer<>(properties);
    }

    public void start() {
        running.set(true);
        worker.start();
    }

    public void stop() {
        running.set(false);
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
            consumer.subscribe(List.of(TOPIC));
            while (running.get()) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(POLL_TIMEOUT)) {
                    processRecord(record);
                }
            }
        } catch (WakeupException e) {
            if (running.get() && log.isErrorEnabled()) {
                log.error("Moderation result consumer was unexpectedly interrupted", e);
            }
        } catch (KafkaException e) {
            if (log.isErrorEnabled()) {
                log.error("Moderation result consumer failed", e);
            }
        } finally {
            closeConsumer();
        }
    }

    private void processRecord(ConsumerRecord<String, byte[]> record) {
        try {
            applyResult(record);
        } catch (IOException | KafkaException e) {
            if (running.get()) {
                if (log.isWarnEnabled()) {
                    log.warn("Could not apply moderation result for {}", record.key(), e);
                }
                consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
        }
    }

    private void applyResult(ConsumerRecord<String, byte[]> record) throws IOException {
        final ModerationResultEvent result;
        try {
            result = codec.deserializeModerationResult(record.value());
        } catch (IOException | IllegalArgumentException e) {
            if (log.isWarnEnabled()) {
                log.warn("Invalid moderation result for {}", record.key(), e);
            }
            return;
        }

        if (Boolean.FALSE.equals(result.passed())) {
            try {
                dao.delete(result.id());
            } catch (NoSuchElementException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Moderated link {} has already been deleted", result.id());
                }
            }
        }
    }

    private void closeConsumer() {
        try {
            consumer.close(CLOSE_TIMEOUT);
        } catch (KafkaException e) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to close moderation result consumer", e);
            }
        }
    }
}
