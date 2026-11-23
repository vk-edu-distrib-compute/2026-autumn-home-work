package company.vk.edu.distrib.compute.masha533.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.format.DateTimeParseException;
import java.util.concurrent.ExecutionException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class ModerationServiceImpl implements ModerationService {
    private final String bootstrapServers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread worker;

    public ModerationServiceImpl(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private LinkCreatedEvent parseEvent(ConsumerRecord<String, String> record) {
        var id = record.key();
        var value = record.value();
        if (id == null || id.isBlank() || value == null) {
            throw new IllegalArgumentException("Missing link ID or event value");
        }

        var parts = value.split("\n", 2);
        if (parts.length != 2 || parts[1].isBlank()) {
            throw new IllegalArgumentException("Expected creation time and URL");
        }

        return new LinkCreatedEvent(id, parts[1], Instant.parse(parts[0]));
    }

    private List<String> loadBannedWords() {
        var stream = ModerationServiceImpl.class.getResourceAsStream("/moderation/banned_words.txt");
        if (stream == null) {
            throw new IllegalStateException("Missing moderation/banned_words.txt");
        }

        try (var reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines()
                    .map(String::strip)
                    .filter(word -> !word.isEmpty())
                    .map(word -> word.toLowerCase(Locale.ROOT))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isBanned(String url, List<String> bannedWords) {
        var decodedUrl = URLDecoder.decode(url.replace("+", "%2B"), StandardCharsets.UTF_8)
                .toLowerCase(Locale.ROOT);

        for (var word : bannedWords) {
            if (decodedUrl.contains(word)) {
                return true;
            }
        }
        return false;
    }

    private ProducerRecord<String, String> createDeadLetterRecord(
        ConsumerRecord<String, String> record, RuntimeException e) {
        final var message = new ProducerRecord<>(
                "links.created.dlt", null, record.key(), record.value(), record.headers()
        );
        message.headers().add("error", e.toString().getBytes(StandardCharsets.UTF_8));
        return message;
    }

    private void consumeLinks() {
        final var bannedWords = loadBannedWords();
        try (var consumer = createConsumer(); var producer = createProducer()) {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    ProducerRecord<String, String> message;
                    try {
                        message = moderate(record, bannedWords);
                    } catch (IllegalArgumentException | DateTimeParseException e) {
                        message = createDeadLetterRecord(record, e);
                    }
                    producer.send(message).get();
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to publish moderation result", e);
        }
    }

    private ProducerRecord<String, String> moderate(ConsumerRecord<String, String> record, List<String> bannedWords) {
        final var event = parseEvent(record);
        final boolean banned = isBanned(event.url(), bannedWords);

        final var result = new ModerationResultEvent(
                event.id(),
                !banned,
                banned ? "URL contains a banned word" : "OK",
                Instant.now()
        );

        final var value = result.passed() + "\n" + result.moderatedAt() + "\n" + result.reason();
        return new ProducerRecord<>("links.moderated", result.id(), value);
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "masha533-moderation");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of("links.created"));
        return consumer;
    }

    private KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    @Override
    public void start() {
        running.set(true);
        worker = new Thread(this::consumeLinks, "masha533-moderation");
        worker.start();
    }

    @Override
    public void stop() {
        running.set(false);
        if (worker != null) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
