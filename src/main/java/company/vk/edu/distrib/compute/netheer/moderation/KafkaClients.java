package company.vk.edu.distrib.compute.netheer.moderation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

final class KafkaClients {
    static final String CREATED = "links.created";
    static final String MODERATED = "links.moderated";
    static final String DEAD_LETTER = "links.created.dlt";

    private KafkaClients() {
    }

    static KafkaProducer<byte[], byte[]> producer(String bootstrapServers, Map<String, Object> extraConfig) {
        Properties config = new Properties();
        config.putAll(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000,
                ProducerConfig.BUFFER_MEMORY_CONFIG, 4 * 1024 * 1024
        ));
        config.putAll(extraConfig);
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    static void consume(
            String bootstrapServers,
            String groupId,
            String topic,
            BiConsumer<KafkaConsumer<byte[], byte[]>, ConsumerRecords<byte[], byte[]>> handleBatch
    ) {
        boolean interrupted = false;
        try (var consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000,
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 16
        ), new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            try {
                consumer.subscribe(List.of(topic));
                while (!Thread.currentThread().isInterrupted()) {
                    var records = consumer.poll(Duration.ofMillis(250));
                    if (!records.isEmpty()) {
                        handleBatch.accept(consumer, records);
                    }
                }
            } finally {
                interrupted = Thread.interrupted();
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
