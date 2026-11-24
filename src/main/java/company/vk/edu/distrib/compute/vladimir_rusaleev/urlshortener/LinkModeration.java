package company.vk.edu.distrib.compute.vladimir_rusaleev.urlshortener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LinkModeration implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LinkModeration.class);
    private static final long RETRY_MILLIS = 100;
    private static final long STOP_MILLIS = 4000;

    private final String servers;
    private final Path directory;
    private final Results results;
    private final AtomicBoolean running = new AtomicBoolean();

    private Thread sender;

    LinkModeration(String servers, Dao<String> links, int port) {
        this.servers = servers;
        directory = Path.of(System.getProperty("user.home"), ".vk-moderation", Integer.toString(port));
        results = new Results(servers, links, port);
    }

    void created(String id, String url) {
        try {
            Files.createDirectories(directory);
            Path temporary = Files.createTempFile(directory, "event-", ".tmp");
            try {
                Files.write(temporary, ModerationEvents.encodeCreated(new LinkCreatedEvent(id, url, Instant.now())));
                Files.move(temporary, directory.resolve(id + ".event"), StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (IOException exception) {
            LOG.error("Could not save moderation event for {}", id, exception);
        }
    }

    void start() {
        running.set(true);
        results.start();
        sender = Thread.ofPlatform().daemon().name("links-created-sender").start(this::run);
    }

    private void run() {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(KafkaWorker.producerProperties(servers))) {
            while (running.get()) {
                try {
                    publish(producer);
                    Thread.sleep(RETRY_MILLIS);
                } catch (IOException | RuntimeException exception) {
                    if (running.get()) {
                        LOG.warn("Could not publish link events; will retry", exception);
                        Thread.sleep(RETRY_MILLIS);
                    }
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException exception) {
            if (running.get()) {
                LOG.error("Link event sender stopped", exception);
            }
        }
    }

    private void publish(KafkaProducer<String, byte[]> producer) throws IOException {
        if (!Files.isDirectory(directory)) {
            return;
        }
        try (var files = Files.newDirectoryStream(directory, "*.event")) {
            for (Path file : files) {
                if (!running.get()) {
                    return;
                }
                publishFile(producer, file);
            }
        }
    }

    private static void publishFile(KafkaProducer<String, byte[]> producer, Path file)
        throws IOException {
        String name = file.getFileName().toString();
        String id = name.substring(0, name.indexOf('.'));
        KafkaWorker.send(producer, new ProducerRecord<>(
            ModerationEvents.CREATED_TOPIC, id, Files.readAllBytes(file)));
        Files.delete(file);
    }

    @Override
    public void close() {
        running.set(false);
        if (sender != null) {
            sender.interrupt();
            try {
                sender.join(STOP_MILLIS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
        results.close();
    }

    private static final class Results extends KafkaWorker {
        private final Dao<String> links;

        Results(String servers, Dao<String> links, int port) {
            super(servers, "vladimir-results-" + port + "-" + UUID.randomUUID(),
                ModerationEvents.MODERATED_TOPIC, false);
            this.links = links;
        }

        @Override
        protected void onRecord(KafkaProducer<String, byte[]> producer, ConsumerRecord<String, byte[]> record)
            throws IOException {
            ModerationResultEvent result;
            try {
                result = ModerationEvents.decodeResult(record.key(), record.value());
            } catch (RuntimeException exception) {
                LOG.warn("Invalid moderation result", exception);
                return;
            }
            if (!result.passed()) {
                links.delete(result.id());
            }
        }
    }
}
