package company.vk.edu.distrib.compute.vagifbaratov.moderation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class KafkaWorker implements AutoCloseable {
    private static final int JOIN_TIMEOUT = 5000;
    private static final int CONSUMER_POOL_TIMEOUT = 200;
    private static final Logger log = LoggerFactory.getLogger(KafkaWorker.class);
    private final String bootstrapServers;
    private final String topic;
    private final String group;
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean();

    protected KafkaWorker(String bootstrapServers, String topic, String group) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.group = group;
        thread = new Thread(this::run, "vagifbaratov-" + topic);
        thread.setDaemon(true);
    }

    public final void start() {
        running.set(true);
        thread.start();
    }

    protected abstract void publishPending(Producer<String, String> producer);

    protected abstract void process(
        ConsumerRecord<String, String> record,
        Producer<String, String> producer
    ) throws IOException, ExecutionException, InterruptedException;

    private void run() {
        try (
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())
        ) {
            try {
                consumer.subscribe(List.of(topic));
                while (running.get()) {
                    try {
                        publishPending(producer);
                        consume(consumer, producer);
                    } catch (RuntimeException exception) {
                        if (running.get()) {
                            if (log.isWarnEnabled()) {
                                log.warn("Kafka iteration failed for {}", topic, exception);
                            }
                            Thread.sleep(200);
                        }
                    }
                }
            } finally {
                Thread.interrupted();
                producer.close(Duration.ZERO);
                consumer.close(Duration.ofSeconds(1));
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException exception) {
            if (log.isWarnEnabled()) {
                log.warn("Kafka worker stopped for {}", topic, exception);
            }
        }
    }

    final void consume(Consumer<String, String> consumer, Producer<String, String> producer) {
        var records = consumer.poll(Duration.ofMillis(CONSUMER_POOL_TIMEOUT));
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, String> record : records.records(partition)) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                if (!processRecord(consumer, producer, partition, record)) {
                    break;
                }
            }
        }
    }

    private boolean processRecord(
        Consumer<String, String> consumer,
        Producer<String, String> producer,
        TopicPartition partition,
        ConsumerRecord<String, String> record
    ) {
        try {
            process(record, producer);
            consumer.commitSync(
                    Map.of(partition, new OffsetAndMetadata(record.offset() + 1)),
                    Duration.ofSeconds(1)
            );
            return true;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception exception) {
            if (log.isWarnEnabled()) {
                log.warn("Unable to process Kafka record from {}", topic, exception);
            }
            consumer.seek(partition, record.offset());
            return false;
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        return properties;
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        return properties;
    }

    @Override
    public final void close() {
        running.set(false);
        thread.interrupt();
        try {
            thread.join(JOIN_TIMEOUT);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) {
            throw new IllegalStateException("Kafka worker did not stop");
        }
    }
}
