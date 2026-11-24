package company.vk.edu.distrib.compute.nosorozhek.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.nosorozhek.urlshortener.UrlShortener;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaModerationClient implements LinkCreatedPublisher {
    static final String CREATED_TOPIC = "links.created";
    static final String MODERATED_TOPIC = "links.moderated";
    static final int N_THREADS = 4;
    private static final Logger log = LoggerFactory.getLogger(KafkaModerationClient.class);
    private static final Duration KAFKA_TIMEOUT = Duration.ofMillis(500);
    private static final long RETRY_DELAY_MS = 100;
    private final String bootstrapServers;
    private final UrlShortener urlShortener;
    private final ModerationEventCodec codec = new ModerationEventCodec();
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;

    private final ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);
    private final CountDownLatch shutdown = new CountDownLatch(1);
    private final AtomicBoolean running = new AtomicBoolean();
    private final Thread consumerThread = Thread.ofPlatform()
            .name("shortener-moderation-results")
            .unstarted(this::consumeResults);
    private final AtomicBoolean stopped = new AtomicBoolean();

    public KafkaModerationClient(String bootstrapServers, UrlShortener urlShortener) {
        this.bootstrapServers = bootstrapServers;
        this.urlShortener = urlShortener;
        consumer = new KafkaConsumer<>(kafkaConsumerProperties());
        producer = new KafkaProducer<>(kafkaProducerProperties());
    }

    private Properties kafkaConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "url-shortener-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        return properties;
    }

    private Properties kafkaProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        return properties;
    }

    public void start() {
        if (stopped.get()) {
            throw new IllegalStateException("Kafka client has been stopped");
        }
        if (!running.get()) {
            running.set(true);
            consumerThread.start();
        }
    }

    @Override
    public void publish(LinkCreatedEvent event) {
        if (stopped.get()) {
            return;
        }
        try {
            executor.submit(() ->
                    producer.send(new ProducerRecord<>(CREATED_TOPIC, event.id(), codec.encodeCreated(event))));
        } catch (RejectedExecutionException e) {
            log.warn("Kafka publisher is stopping", e);
        }
    }

    private void consumeResults() {
        try {
            consumer.subscribe(Collections.singletonList(MODERATED_TOPIC));
            while (running.get()) {
                try {
                    consumeResultBatch();
                } catch (KafkaException e) {
                    if (running.get()) {
                        log.warn("Failed to consume moderation results", e);
                        if (shutdown.await(RETRY_DELAY_MS, TimeUnit.MILLISECONDS)) {
                            return;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeConsumer();
        }
    }

    private void consumeResultBatch() throws InterruptedException {
        for (ConsumerRecord<String, String> record : consumer.poll(KAFKA_TIMEOUT)) {
            if (!running.get() || !applyRecord(record)) {
                if (shutdown.await(RETRY_DELAY_MS, TimeUnit.MILLISECONDS)) {
                    return;
                }
                break;
            }
        }
    }

    private boolean applyRecord(ConsumerRecord<String, String> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        try {
            applyResult(codec.decodeResult(record.value()));
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)), KAFKA_TIMEOUT);
            return true;
        } catch (IOException | RuntimeException e) {
            if (running.get()) {
                consumer.seek(partition, record.offset());
                log.warn("Failed to apply moderation result", e);
            }
            return false;
        }
    }

    private void closeConsumer() {
        boolean interrupted = Thread.interrupted();
        try {
            consumer.close(CloseOptions.timeout(KAFKA_TIMEOUT));
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void applyResult(ModerationResultEvent event) throws IOException {
        if (!event.passed()) {
            urlShortener.delete(event.id());
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    public void stop() {
        if (stopped.get()) {
            return;
        }
        stopped.set(true);
        running.set(false);
        shutdown.countDown();
        consumer.wakeup();
        consumerThread.interrupt();
        executor.shutdown();
        try {
            if (consumerThread.getState() == Thread.State.NEW) {
                closeConsumer();
            } else if (!consumerThread.join(Duration.ofSeconds(2))) {
                throw new IllegalStateException("Kafka consumer did not stop");
            }
            if (!executor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException("Kafka publisher did not stop");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping Kafka client", e);
        } finally {
            executor.shutdownNow();
            producer.close(KAFKA_TIMEOUT);
        }
    }
}
