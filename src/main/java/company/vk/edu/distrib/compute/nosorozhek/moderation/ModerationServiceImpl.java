package company.vk.edu.distrib.compute.nosorozhek.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ModerationServiceImpl implements ModerationService {
    static final String CREATED_TOPIC = "links.created";
    static final String MODERATED_TOPIC = "links.moderated";
    static final String DLT_TOPIC = "links.created.dlt";
    private static final String ERROR_HEADER = "error";
    private static final Logger log = LoggerFactory.getLogger(ModerationServiceImpl.class);
    private static final Duration KAFKA_TIMEOUT = Duration.ofMillis(500);
    private static final long RETRY_DELAY_MS = 100;
    private final String bootstrapServers;
    private final ModerationEventCodec codec = new ModerationEventCodec();
    private final BannedWordsModerator moderator = new BannedWordsModerator();
    private final Consumer<String, String> consumer;
    private final Producer<String, String> producer;
    private final CountDownLatch shutdown = new CountDownLatch(1);
    private final AtomicBoolean running = new AtomicBoolean();
    private final Thread consumerThread = Thread.ofPlatform()
            .name("link-moderation")
            .unstarted(this::consumeCreatedLinks);
    private boolean stopped;

    public ModerationServiceImpl(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        consumer = new KafkaConsumer<>(kafkaConsumerProperties());
        producer = new KafkaProducer<>(kafkaProducerProperties());
    }

    private Properties kafkaConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "nosorozhek-moderation");
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

    @Override
    public void start() {
        if (stopped) {
            throw new IllegalStateException("Moderation service has been stopped");
        }
        if (!running.get()) {
            running.set(true);
            consumerThread.start();
        }
    }

    private void consumeCreatedLinks() {
        try {
            consumer.subscribe(Collections.singletonList(CREATED_TOPIC));
            while (running.get()) {
                try {
                    consumeCreatedBatch();
                } catch (KafkaException e) {
                    if (running.get()) {
                        log.warn("Failed to consume created links", e);
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

    private void consumeCreatedBatch() throws InterruptedException {
        for (ConsumerRecord<String, String> record : consumer.poll(KAFKA_TIMEOUT)) {
            if (!running.get() || !processRecord(record)) {
                if (shutdown.await(RETRY_DELAY_MS, TimeUnit.MILLISECONDS)) {
                    return;
                }
                break;
            }
        }
    }

    private boolean processRecord(ConsumerRecord<String, String> record) throws InterruptedException {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        try {
            process(record);
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)), KAFKA_TIMEOUT);
            return true;
        } catch (ExecutionException | RuntimeException e) {
            if (running.get()) {
                consumer.seek(partition, record.offset());
                log.warn("Failed to moderate link", e);
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

    private void process(ConsumerRecord<String, String> record) throws ExecutionException, InterruptedException {
        ModerationResultEvent result;
        try {
            LinkCreatedEvent event = codec.decodeCreated(record.value());
            result = moderator.moderate(event);
        } catch (RuntimeException e) {
            ProducerRecord<String, String> deadLetter = new ProducerRecord<>(DLT_TOPIC, record.key(), record.value());
            deadLetter.headers().add(ERROR_HEADER, e.toString().getBytes(StandardCharsets.UTF_8));
            producer.send(deadLetter).get();
            return;
        }
        producer.send(new ProducerRecord<>(MODERATED_TOPIC, result.id(), codec.encodeResult(result))).get();
    }

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        running.set(false);
        shutdown.countDown();
        consumer.wakeup();
        consumerThread.interrupt();
        try {
            if (consumerThread.getState() == Thread.State.NEW) {
                closeConsumer();
            } else if (!consumerThread.join(Duration.ofSeconds(2))) {
                throw new IllegalStateException("Moderation consumer thread failed to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping moderation service", e);
        } finally {
            producer.close(KAFKA_TIMEOUT);
        }
    }
}
