package company.vk.edu.distrib.compute.k0oshara.urlshortener;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

final class ModerationKafka {
    static final String CREATED = "links.created";
    static final String MODERATED = "links.moderated";
    static final String DEAD_LETTER = "links.created.dlt";

    private ModerationKafka() {
    }

    static KafkaProducer<byte[], byte[]> producer(String servers) {
        return new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000
        ), new ByteArraySerializer(), new ByteArraySerializer());
    }

    static KafkaConsumer<byte[], byte[]> consumer(String servers, String groupId) {
        return new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers,
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false
        ), new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    static byte[] encode(LinkCreatedEvent event) {
        return (event.createdAt() + "\n" + event.url()).getBytes(StandardCharsets.UTF_8);
    }

    static byte[] encode(ModerationResultEvent event) {
        return String.join("\n", event.moderatedAt().toString(), event.passed().toString(),
            event.reason() == null ? "" : event.reason())
            .getBytes(StandardCharsets.UTF_8);
    }

    static LinkCreatedEvent decodeCreated(byte[] key, byte[] value) {
        String[] fields = decode(value, 2);
        return new LinkCreatedEvent(decodeKey(key), fields[1], timestamp(fields[0]));
    }

    static ModerationResultEvent decodeResult(byte[] key, byte[] value) {
        String[] fields = decode(value, 3);
        boolean passed = switch (fields[1]) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("Invalid moderation decision");
        };
        return new ModerationResultEvent(decodeKey(key), passed, fields[2], timestamp(fields[0]));
    }

    private static String[] decode(byte[] value, int fieldCount) {
        String[] fields = decodeUtf8(value).split("\n", fieldCount);
        if (fields.length != fieldCount) {
            throw new IllegalArgumentException("Invalid event format");
        }
        return fields;
    }

    private static String decodeKey(byte[] key) {
        String id = decodeUtf8(key);
        if (id.isBlank()) {
            throw new IllegalArgumentException("Missing event ID");
        }
        return id;
    }

    private static String decodeUtf8(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Missing event key or value");
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException exception) {
            throw new IllegalArgumentException("Invalid UTF-8 event", exception);
        }
    }

    private static Instant timestamp(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            throw new IllegalArgumentException("Invalid event timestamp", exception);
        }
    }
}
