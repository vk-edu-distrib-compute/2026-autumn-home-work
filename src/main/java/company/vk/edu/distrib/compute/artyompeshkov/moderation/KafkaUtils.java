package company.vk.edu.distrib.compute.artyompeshkov.moderation;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class KafkaUtils {
    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(1);
    static final Duration RETRY_DELAY = Duration.ofMillis(500);
    static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    static final String CREATED_TOPIC = "links.created";
    static final String MODERATED_TOPIC = "links.moderated";
    static final String DEAD_LETTER_TOPIC = "links.created.dlt";

    private KafkaUtils() {
    }

    static KafkaProducer<String, byte[]> producer(String bootstrapServers) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    static KafkaConsumer<String, byte[]> consumer(String bootstrapServers, String group) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false));
    }

    static void stopWorker(Thread worker) {
        worker.interrupt();
        boolean interrupted = Thread.interrupted();
        try {
            if (worker.isAlive() && !worker.join(STOP_TIMEOUT) && log.isWarnEnabled()) {
                log.warn("Worker {} is still finishing its current operation", worker.getName());
            }
        } catch (InterruptedException e) {
            interrupted = true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
