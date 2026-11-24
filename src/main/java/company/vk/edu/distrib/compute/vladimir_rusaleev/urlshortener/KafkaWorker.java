package company.vk.edu.distrib.compute.vladimir_rusaleev.urlshortener;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class KafkaWorker implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaWorker.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final long STOP_TIMEOUT_MILLIS = 5000;

    private final String servers;
    private final String group;
    private final String topic;
    private final boolean transactional;
    private final AtomicBoolean running = new AtomicBoolean();

    private Thread thread;

    KafkaWorker(String servers, String group, String topic, boolean transactional) {
        this.servers = servers;
        this.group = group;
        this.topic = topic;
        this.transactional = transactional;
    }

    public final void start() {
        if (thread != null) {
            throw new IllegalStateException("Worker already started");
        }
        running.set(true);
        thread = Thread.ofPlatform().daemon().name(group).start(this::run);
    }

    private void run() {
        try (Clients clients = new Clients(servers, group, transactional)) {
            if (transactional) {
                clients.producer.initTransactions();
            }
            clients.consumer.subscribe(List.of(topic));
            while (running.get()) {
                poll(clients);
            }
        } catch (RuntimeException exception) {
            if (running.get()) {
                LOG.error("Kafka worker stopped", exception);
            }
        }
    }

    private void poll(Clients clients) {
        try {
            for (ConsumerRecord<String, byte[]> record : clients.consumer.poll(POLL_TIMEOUT)) {
                process(clients, record);
            }
        } catch (RuntimeException exception) {
            if (running.get()) {
                LOG.warn("Kafka read failed", exception);
            }
        }
    }

    private void process(Clients clients, ConsumerRecord<String, byte[]> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        try {
            if (transactional) {
                clients.producer.beginTransaction();
            }

            onRecord(clients.producer, record);

            Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
                offsets.put(partition, new OffsetAndMetadata(record.offset() + 1));

            if (transactional) {
                clients.producer.sendOffsetsToTransaction(offsets, clients.consumer.groupMetadata());
                clients.producer.commitTransaction();
            } else {
                clients.consumer.commitSync(offsets);
            }
        } catch (IOException | RuntimeException exception) {
            if (transactional) {
                Thread.interrupted();
                clients.producer.abortTransaction();
            }
            clients.consumer.seek(partition, record.offset());
            if (running.get()) {
                LOG.warn("Kafka processing failed; record will be retried", exception);
            }
        }
    }

    protected abstract void onRecord(
        KafkaProducer<String, byte[]> producer,
        ConsumerRecord<String, byte[]> record
    ) throws IOException;

    static void send(KafkaProducer<String, byte[]> producer, ProducerRecord<String, byte[]> record)
        throws IOException {
        try {
            producer.send(record).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Kafka send interrupted", exception);
        } catch (ExecutionException exception) {
            throw new IOException("Kafka send failed", exception);
        }
    }

    static Properties producerProperties(String servers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("max.block.ms", 1000);
        properties.put("request.timeout.ms", 1000);
        properties.put("delivery.timeout.ms", 5000);
        properties.put("linger.ms", 0);
        properties.put("buffer.memory", 1024 * 1024);
        return properties;
    }

    private static Properties consumerProperties(String servers, String group) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", servers);
        properties.put("group.id", group);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", false);
        properties.put("allow.auto.create.topics", false);
        properties.put("max.poll.records", 1);
        properties.put("default.api.timeout.ms", 1000);
        properties.put("isolation.level", "read_committed");
        return properties;
    }

    @Override
    public final void close() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(STOP_TIMEOUT_MILLIS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class Clients implements AutoCloseable {
        private final KafkaProducer<String, byte[]> producer;
        private final KafkaConsumer<String, byte[]> consumer;

        Clients(String servers, String group, boolean transactional) {
            Properties properties = producerProperties(servers);
            if (transactional) {
                properties.put("transactional.id", group + "-" + UUID.randomUUID());
                properties.put("max.block.ms", 30000);
                properties.put("transaction.timeout.ms", 10000);
            }
            producer = new KafkaProducer<>(properties);
            consumer = new KafkaConsumer<>(consumerProperties(servers, group));
        }

        @Override
        @SuppressWarnings("PMD.UseTryWithResources")
        public void close() {
            Thread.interrupted();
            try {
                consumer.close(CLOSE_TIMEOUT);
            } finally {
                producer.close(CLOSE_TIMEOUT);
            }
        }
    }
}
