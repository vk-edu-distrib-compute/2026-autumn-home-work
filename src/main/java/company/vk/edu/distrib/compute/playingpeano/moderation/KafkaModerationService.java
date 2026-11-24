package company.vk.edu.distrib.compute.playingpeano.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationService;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaModerationService implements ModerationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaModerationService.class);
    private static final String LINKS_CREATED_TOPIC = "links.created";
    private static final String CONSUMER_GROUP_ID = "playingpeano-link-moderation";
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(2);

    private final Lock lifecycleLock = new ReentrantLock();
    private final String bootstrapServers;
    private final ModerationRecordProcessor processor;
    private final Thread worker;
    private final AtomicBoolean running = new AtomicBoolean();
    private boolean started;

    KafkaModerationService(String bootstrapServers, Set<String> bannedWords) {
        this.bootstrapServers = bootstrapServers;
        processor = new ModerationRecordProcessor(bannedWords);
        worker = Thread.ofPlatform().daemon().name("playingpeano-moderation").unstarted(this::run);
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            if (started) {
                throw new IllegalStateException("Moderation service has already been started");
            }
            started = true;
            running.set(true);
            worker.start();
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            running.set(false);
            if (!started) {
                return;
            }
            worker.interrupt();
        } finally {
            lifecycleLock.unlock();
        }
        try {
            worker.join(STOP_TIMEOUT);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        try {
            while (running.get()) {
                try {
                    consumeRecords();
                } catch (KafkaException exception) {
                    if (running.get()) {
                        LOGGER.warn("Moderation Kafka client failed, reconnecting", exception);
                        Thread.sleep(POLL_INTERVAL);
                    }
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void consumeRecords() throws InterruptedException {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties());
             KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(LINKS_CREATED_TOPIC));
            while (running.get()) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL_INTERVAL)) {
                    processRecord(record, producer, consumer);
                }
            }
        }
    }

    private void processRecord(
        ConsumerRecord<byte[], byte[]> record,
        KafkaProducer<byte[], byte[]> producer,
        KafkaConsumer<byte[], byte[]> consumer
    ) throws InterruptedException {
        try {
            producer.send(processor.outputFor(record)).get();
        } catch (ExecutionException exception) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Cannot publish moderation outcome for offset {}", record.offset(), exception.getCause());
            }
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            Thread.sleep(POLL_INTERVAL);
            return;
        }
        while (running.get()) {
            try {
                consumer.commitSync(COMMIT_TIMEOUT);
                return;
            } catch (TimeoutException exception) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Cannot commit moderation offset {}", record.offset(), exception);
                }
            }
        }
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000);
        return properties;
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }
}
