package company.vk.edu.distrib.compute.cloudyy74.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public final class Cloudyy74KafkaUtils {
    public static final String CREATED_TOPIC = "links.created";
    public static final String MODERATED_TOPIC = "links.moderated";
    public static final String DEAD_LETTER_TOPIC = "links.created.dlt";
    public static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    public static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final int CREATED_PARTS = 3;
    private static final int MODERATED_PARTS = 4;

    private Cloudyy74KafkaUtils() {
    }

    public static KafkaProducer<String, String> createProducer(String bootstrapServers) {
        final var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000);
        return new KafkaProducer<>(properties);
    }

    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        final var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        return new KafkaConsumer<>(properties);
    }

    public static String serialize(LinkCreatedEvent event) {
        return event.id() + '\n' + event.createdAt() + '\n' + event.url();
    }

    public static LinkCreatedEvent readCreated(String value) {
        final var parts = value.split("\n", CREATED_PARTS);
        if (parts.length != CREATED_PARTS) {
            throw new IllegalArgumentException("Invalid link created event");
        }
        return new LinkCreatedEvent(parts[0], parts[2], Instant.parse(parts[1]));
    }

    public static String serialize(ModerationResultEvent event) {
        return event.id() + '\n' + event.passed() + '\n' + event.moderatedAt() + '\n' + event.reason();
    }

    public static ModerationResultEvent readModerated(String value) {
        final var parts = value.split("\n", MODERATED_PARTS);
        if (parts.length != MODERATED_PARTS) {
            throw new IllegalArgumentException("Invalid moderation result event");
        }
        final var passed = switch (parts[1]) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("Invalid moderation result");
        };
        return new ModerationResultEvent(parts[0], passed, parts[3], Instant.parse(parts[2]));
    }
}
