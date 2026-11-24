package company.vk.edu.distrib.compute.flighen.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class FlighenLinkEventProducer {
    private static final Logger log = LoggerFactory.getLogger(FlighenLinkEventProducer.class);
    private static final String TOPIC = "links.created";
    private static final int QUEUE_CAPACITY = 1024;
    private static final int MAX_BLOCK_MS = 500;
    private static final Duration CLOSE_TIMEOUT = Duration.ofMillis(500);

    private final ModerationEventCodec codec = new ModerationEventCodec();
    private final KafkaProducer<String, byte[]> producer;
    private final ExecutorService executor = new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(QUEUE_CAPACITY)
    );

    public FlighenLinkEventProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        producer = new KafkaProducer<>(properties);
    }

    public void send(String id, String url) {
        LinkCreatedEvent event = new LinkCreatedEvent(id, url, Instant.now());
        try {
            executor.execute(() -> sendEvent(event));
        } catch (RejectedExecutionException e) {
            if (log.isWarnEnabled()) {
                log.warn("Could not enqueue link creation event for {}", id, e);
            }
        }
    }

    private void sendEvent(LinkCreatedEvent event) {
        try {
            byte[] data = codec.serializeLinkCreated(event);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, event.id(), data);
            producer.send(record, (metadata, error) -> {
                if (error != null && log.isWarnEnabled()) {
                    log.warn("Could not deliver link creation event for {}", event.id(), error);
                }
            });
        } catch (IOException | RuntimeException e) {
            if (log.isWarnEnabled()) {
                log.warn("Could not send link creation event for {}", event.id(), e);
            }
        }
    }

    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            producer.close(CLOSE_TIMEOUT);
        } catch (RuntimeException e) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to close Kafka producer", e);
            }
        }
    }
}
