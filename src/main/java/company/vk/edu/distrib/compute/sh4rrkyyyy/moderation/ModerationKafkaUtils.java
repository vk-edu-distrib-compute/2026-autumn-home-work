package company.vk.edu.distrib.compute.sh4rrkyyyy.moderation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

final class ModerationKafkaUtils {
    static final String CREATED = "links.created";
    static final String MODERATED = "links.moderated";
    static final String CREATED_DLT = "links.created.dlt";
    static final String BANNED_WORD_REASON = "Banned word: ";
    static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private ModerationKafkaUtils() {
    }

    static KafkaProducer<byte[], byte[]> producer(String bootstrapServers) {
        return new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                new ByteArraySerializer(), new ByteArraySerializer());
    }

    static KafkaConsumer<byte[], byte[]> consumer(String bootstrapServers, String group) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    static void stop(Thread worker) {
        worker.interrupt();
        try {
            worker.join(Duration.ofSeconds(1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
