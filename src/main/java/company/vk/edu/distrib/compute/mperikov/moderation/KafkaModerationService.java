package company.vk.edu.distrib.compute.mperikov.moderation;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaModerationService implements ModerationService {
    private static final Logger log = LoggerFactory.getLogger(KafkaModerationService.class);
    private static final int SEND_SECONDS = 10;

    private final String servers;
    private final BannedWords words;
    private final CountDownLatch joined = new CountDownLatch(1);
    private @Nullable KafkaProducer<String, String> producer;
    private @Nullable KafkaConsumer<String, String> consumer;
    private @Nullable Thread worker;
    private final AtomicBoolean running = new AtomicBoolean();

    KafkaModerationService(String servers, BannedWords words) {
        this.servers = servers;
        this.words = words;
    }

    @Override
    public void start() {
        if (running.get()) {
            throw new IllegalStateException("Moderation service is already started");
        }
        producer = KafkaClients.producer(servers, 5_000, 5_000);
        consumer = subscribed();
        running.set(true);
        Thread thread = new Thread(this::loop, "mperikov-moderation");
        thread.setDaemon(true);
        worker = thread;
        thread.start();
        awaitJoin();
    }

    @Override
    public void stop() {
        running.set(false);
        KafkaConsumer<String, String> current = consumer;
        if (current != null) {
            current.wakeup();
        }
        join();
        close();
    }

    private KafkaConsumer<String, String> subscribed() {
        KafkaConsumer<String, String> created = KafkaClients.consumer(servers, KafkaClients.MODERATION_GROUP);
        created.subscribe(List.of(KafkaClients.CREATED), new JoinListener());
        return created;
    }

    private void awaitJoin() {
        try {
            if (!joined.await(SEND_SECONDS, TimeUnit.SECONDS) && log.isWarnEnabled()) {
                log.warn("Moderation consumer did not join the group");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        try {
            while (running.get()) {
                poll();
            }
        } catch (WakeupException ex) {
            if (running.get()) {
                throw ex;
            }
        }
    }

    private void poll() {
        KafkaConsumer<String, String> current = consumer;
        if (current == null) {
            return;
        }
        ConsumerRecords<String, String> records = current.poll(Duration.ofMillis(200));
        for (ConsumerRecord<String, String> record : records) {
            handle(record);
        }
        if (!records.isEmpty()) {
            current.commitSync();
        }
    }

    private void handle(ConsumerRecord<String, String> record) {
        try {
            moderate(record);
        } catch (RuntimeException ex) {
            deadLetter(record, ex);
        }
    }

    private void moderate(ConsumerRecord<String, String> record) {
        String value = record.value();
        if (value == null) {
            throw new IllegalArgumentException("empty created event");
        }
        LinkCreatedEvent event = EventTexts.createdEvent(value);
        Optional<String> banned = words.match(event.url());
        String reason = banned.orElse("ok");
        ModerationResultEvent result = new ModerationResultEvent(
            event.id(), banned.isEmpty(), reason, Instant.now());
        send(new ProducerRecord<>(KafkaClients.MODERATED, event.id(), EventTexts.moderated(result)));
    }

    private void deadLetter(ConsumerRecord<String, String> record, RuntimeException ex) {
        String value = record.value() == null ? "" : record.value();
        String key = record.key() == null ? "" : record.key();
        ProducerRecord<String, String> copy = new ProducerRecord<>(KafkaClients.DEAD, key, value);
        copy.headers().add(KafkaClients.ERROR, reason(ex).getBytes(StandardCharsets.UTF_8));
        try {
            send(copy);
        } catch (RuntimeException sendError) {
            if (log.isWarnEnabled()) {
                log.warn("Dead letter was not sent", sendError);
            }
        }
    }

    private void send(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> current = producer;
        if (current == null) {
            throw new IllegalStateException("Moderation producer is closed");
        }
        try {
            current.send(record).get(SEND_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Moderation send was interrupted", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Moderation send failed", ex);
        }
    }

    private void join() {
        Thread thread = worker;
        if (thread == null) {
            return;
        }
        try {
            thread.join(Duration.ofSeconds(2));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void close() {
        KafkaConsumer<String, String> currentConsumer = consumer;
        if (currentConsumer != null) {
            currentConsumer.close();
        }
        KafkaProducer<String, String> currentProducer = producer;
        if (currentProducer != null) {
            currentProducer.close();
        }
    }

    private static String reason(RuntimeException ex) {
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            return ex.getClass().getSimpleName();
        }
        return message;
    }

    private final class JoinListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (log.isDebugEnabled()) {
                log.debug("Moderation partitions revoked {}", partitions.size());
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            joined.countDown();
        }
    }
}
