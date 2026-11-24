package company.vk.edu.distrib.compute.netheer.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.CharacterCodingException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class LinkModeration {
    private static final Logger LOG = LoggerFactory.getLogger(LinkModeration.class);
    private static final int QUEUE_CAPACITY = 256;

    private final BlockingQueue<LinkCreatedEvent> pending = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final KafkaWorker publisher;
    private final KafkaWorker resultsConsumer;
    private final LinkDeletion deleteLink;

    public LinkModeration(String bootstrapServers, LinkDeletion deleteLink) {
        this.deleteLink = deleteLink;
        String groupId = "netheer-shortener-" + UUID.randomUUID();
        this.publisher = new KafkaWorker("link-created-publisher", () -> publishCreatedLinks(bootstrapServers));
        this.resultsConsumer = new KafkaWorker("moderation-results-consumer",
                () -> KafkaClients.consume(bootstrapServers, groupId, KafkaClients.MODERATED, (consumer, records) -> {
                    for (var record : records) {
                        applyResult(record);
                    }
                    consumer.commitSync(Duration.ofSeconds(5));
                }));
    }

    public void start() {
        resultsConsumer.start();
        publisher.start();
    }

    public void stop() {
        publisher.stop();
        resultsConsumer.stop();
    }

    public void submit(String id, String url) {
        if (!pending.offer(new LinkCreatedEvent(id, url, Instant.now())) && LOG.isWarnEnabled()) {
            LOG.warn("Moderation queue is full, creation event for {} was dropped", id);
        }
    }

    private void publishCreatedLinks(String bootstrapServers) {
        try (var producer = KafkaClients.producer(bootstrapServers, Map.of())) {
            while (!Thread.currentThread().isInterrupted()) {
                LinkCreatedEvent created = pending.take();
                sendCreated(producer, created);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendCreated(KafkaProducer<byte[], byte[]> producer, LinkCreatedEvent created) {
        var message = new ProducerRecord<>(KafkaClients.CREATED, ModerationEvents.bytes(created.id()),
                ModerationEvents.encodeCreated(created));
        producer.send(message, (metadata, error) -> {
            if (error != null && LOG.isWarnEnabled()) {
                LOG.warn("Creation event for {} could not be delivered", created.id(), error);
            }
        });
    }

    private void applyResult(ConsumerRecord<byte[], byte[]> record) {
        try {
            ModerationResultEvent result = ModerationEvents.decodeResult(record.key(), record.value());
            if (!result.passed()) {
                deleteLink.delete(result.id());
            }
        } catch (CharacterCodingException | IllegalArgumentException | DateTimeException exception) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Ignoring malformed moderation result at offset {}", record.offset(), exception);
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @FunctionalInterface
    public interface LinkDeletion {
        void delete(String id) throws IOException;
    }
}
