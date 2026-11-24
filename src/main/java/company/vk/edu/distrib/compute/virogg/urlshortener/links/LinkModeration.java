package company.vk.edu.distrib.compute.virogg.urlshortener.links;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LinkModeration implements AutoCloseable {
    private static final int QUEUE_CAPACITY = 1024;
    private static final Duration RETRY_DELAY = Duration.ofMillis(100);
    private static final Logger log = LoggerFactory.getLogger(LinkModeration.class);

    private final String bootstrapServers;
    private final BlockingQueue<LinkCreatedEvent> pending = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    LinkModeration(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    void start(LinksHandler links) {
        workers.execute(this::sendCreated);
        String group = "virogg-shortener-" + UUID.randomUUID();
        workers.execute(() -> ModerationKafka.consume(bootstrapServers, group, ModerationKafka.MODERATED,
                record -> applyResult(links, record)));
    }

    void publish(LinkCreatedEvent event) {
        if (!pending.offer(event) && log.isWarnEnabled()) {
            log.warn("Moderation queue is full for link {}", event.id());
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources") // Kafka default producer close has no deadline 
    private void sendCreated() {
        try {
            KafkaProducer<byte[], byte[]> producer = ModerationKafka.producer(bootstrapServers);
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    send(producer, pending.take());
                }
            } finally {
                ModerationKafka.close(producer);
            }
        } catch (InterruptedException | InterruptException expected) {
            Thread.currentThread().interrupt();
        } catch (KafkaException e) {
            log.warn("Link creation publisher stopped", e);
        }
    }

    private static void send(KafkaProducer<byte[], byte[]> producer, LinkCreatedEvent event)
            throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                producer.send(ModerationKafka.created(event)).get();
                return;
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof RetriableException)) {
                    if (log.isWarnEnabled()) {
                        log.warn("Could not publish link {}", event.id(), e.getCause());
                    }
                    return;
                }
                Thread.sleep(RETRY_DELAY);
            }
        }
    }

    private static void applyResult(LinksHandler links, ConsumerRecord<byte[], byte[]> record) throws IOException {
        ModerationResultEvent result;
        try {
            result = ModerationKafka.readModerated(record);
        } catch (IllegalArgumentException | DateTimeException e) {
            if (log.isWarnEnabled()) {
                log.warn("Ignoring invalid moderation result at offset {}", record.offset(), e);
            }
            return;
        }
        if (!result.passed()) {
            links.remove(result.id());
        }
    }

    @Override
    public void close() {
        workers.shutdownNow();
        workers.close();
    }
}
