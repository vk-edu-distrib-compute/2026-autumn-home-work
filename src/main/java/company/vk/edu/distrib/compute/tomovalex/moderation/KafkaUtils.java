package company.vk.edu.distrib.compute.tomovalex.moderation;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

final class KafkaUtils {
    static final String CREATED_TOPIC = "links.created";
    static final String MODERATED_TOPIC = "links.moderated";
    static final String DEAD_LETTER_TOPIC = "links.created.dlt";
    static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

    private KafkaUtils() {
    }

    static KafkaProducer<String, byte[]> createProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500);
        return new KafkaProducer<>(properties);
    }

    static KafkaConsumer<String, byte[]> createConsumer(String bootstrapServers, String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        return new KafkaConsumer<>(properties);
    }

    static TopicPartition getPartition(ConsumerRecord<String, byte[]> record) {
        return new TopicPartition(record.topic(), record.partition());
    }

    static void commit(KafkaConsumer<String, byte[]> consumer, ConsumerRecord<String, byte[]> record) {
        consumer.commitSync(Map.of(getPartition(record), new OffsetAndMetadata(record.offset() + 1)));
    }

    static void release(KafkaConsumer<String, byte[]> consumer, KafkaProducer<String, byte[]> producer) {
        releaseConsumer(consumer);
        releaseProducer(producer);
    }

    static void releaseConsumer(KafkaConsumer<String, byte[]> consumer) {
        Thread.interrupted();
        try {
            consumer.close(CloseOptions.timeout(Duration.ofMillis(500)));
        } catch (RuntimeException e) {
            log.warn("Could not close Kafka consumer", e);
        }
    }

    static void releaseProducer(KafkaProducer<String, byte[]> producer) {
        Thread.interrupted();
        producer.close(Duration.ZERO);
    }
}
