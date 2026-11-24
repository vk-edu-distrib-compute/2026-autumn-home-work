package company.vk.edu.distrib.compute.sh4rrkyyyy.moderation;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public final class LinkModeration {
    private static final int QUEUE_CAPACITY = 1024;

    private final String bootstrapServers;
    private final ExecutorService requests;
    private final Supplier<Dao<String>> linksDao;
    private final BlockingQueue<LinkCreatedEvent> createdLinks = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread publisher = new Thread(this::publish, "link-events");
    private final Thread receiver = new Thread(this::receive, "link-results");

    public LinkModeration(String bootstrapServers, ExecutorService requests, Supplier<Dao<String>> linksDao) {
        this.bootstrapServers = bootstrapServers;
        this.requests = requests;
        this.linksDao = linksDao;
    }

    public void start() {
        publisher.start();
        receiver.start();
    }

    public void stop() {
        ModerationKafkaUtils.stop(publisher);
        ModerationKafkaUtils.stop(receiver);
    }

    public void linkCreated(String id, String url) {
        createdLinks.offer(new LinkCreatedEvent(id, url, Instant.now()));
    }

    private void publish() {
        try (var producer = ModerationKafkaUtils.producer(bootstrapServers)) {
            while (!Thread.currentThread().isInterrupted()) {
                var event = createdLinks.take();
                producer.send(createdRecord(event));
            }
        } catch (InterruptedException | InterruptException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ProducerRecord<byte[], byte[]> createdRecord(LinkCreatedEvent event) {
        return new ProducerRecord<>(ModerationKafkaUtils.CREATED, null,
                event.createdAt().toEpochMilli(), ModerationKafkaUtils.bytes(event.id()),
                ModerationKafkaUtils.bytes(event.url()));
    }

    private void receive() {
        String group = "sh4rrkyyyy-shortener-" + UUID.randomUUID();
        try (var consumer = ModerationKafkaUtils.consumer(bootstrapServers, group)) {
            consumer.subscribe(List.of(ModerationKafkaUtils.MODERATED));
            while (!Thread.currentThread().isInterrupted()) {
                for (var record : consumer.poll(ModerationKafkaUtils.POLL_TIMEOUT)) {
                    removeRejectedLink(record);
                }
            }
        } catch (InterruptedException | InterruptException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private void removeRejectedLink(ConsumerRecord<byte[], byte[]> record)
            throws ExecutionException, InterruptedException {
        if (record.key() != null && record.value() != null && record.value().length > 0) {
            String id = ModerationKafkaUtils.text(record.key());
            requests.submit(() -> {
                linksDao.get().delete(id);
                return null;
            }).get();
        }
    }
}
