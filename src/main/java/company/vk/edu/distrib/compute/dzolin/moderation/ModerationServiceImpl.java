package company.vk.edu.distrib.compute.dzolin.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ModerationServiceImpl implements ModerationService {
    private static final Logger LOG = LoggerFactory.getLogger(ModerationServiceImpl.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final List<String> bannedWords;
    private Thread worker;

    ModerationServiceImpl(String bootstrapServers) throws IOException {
        try (InputStream input = ModerationServiceImpl.class.getClassLoader()
            .getResourceAsStream("moderation/banned_words.txt")) {
            if (input == null) {
                throw new IOException("Banned words resource is missing");
            }
            bannedWords = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
                .map(String::trim)
                .filter(word -> !word.isEmpty())
                .map(word -> word.toLowerCase(Locale.ROOT))
                .toList();
        }

        consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, "dzolin-moderation",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
        producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));
    }

    @Override
    public void start() {
        worker = Thread.ofVirtual().name("dzolin-moderation").start(this::consume);
    }

    private void consume() {
        try (consumer; producer) {
            consumer.subscribe(List.of(ModerationEvents.CREATED_TOPIC));
            boolean polling = true;
            while (polling) {
                try {
                    var records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, String> record : records) {
                        process(record);
                    }
                    if (!records.isEmpty()) {
                        consumer.commitSync();
                    }
                } catch (WakeupException exception) {
                    polling = false;
                }
            }
        } catch (Exception exception) {
            LOG.error("Moderation consumer stopped unexpectedly", exception);
        }
    }

    private void process(ConsumerRecord<String, String> record) throws ExecutionException, InterruptedException {
        ModerationResultEvent result;
        try {
            result = moderate(record);
        } catch (RuntimeException exception) {
            ProducerRecord<String, String> deadLetter = new ProducerRecord<>(
                ModerationEvents.DEAD_LETTER_TOPIC, record.key(), record.value());
            record.headers().forEach(header -> deadLetter.headers().add(header));
            deadLetter.headers().add("error", exception.toString().getBytes(StandardCharsets.UTF_8));
            send(deadLetter);
            return;
        }
        send(new ProducerRecord<>(ModerationEvents.MODERATED_TOPIC, result.id(),
            ModerationEvents.encode(result)));
    }

    private ModerationResultEvent moderate(ConsumerRecord<String, String> record) {
        LinkCreatedEvent created = decode(record);
        String url = URLDecoder.decode(created.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        String bannedWord = bannedWords.stream().filter(url::contains).findFirst().orElse("");
        boolean passed = bannedWord.isEmpty();
        return new ModerationResultEvent(created.id(), passed,
            passed ? "" : "Banned word: " + bannedWord, Instant.now());
    }

    private LinkCreatedEvent decode(ConsumerRecord<String, String> record) {
        LinkCreatedEvent created = ModerationEvents.decodeCreated(record.value());
        if (!created.id().equals(record.key()) || created.url().isBlank()) {
            throw new IllegalArgumentException("Invalid link created event fields");
        }
        return created;
    }

    private void send(ProducerRecord<String, String> record) throws ExecutionException, InterruptedException {
        producer.send(record).get();
    }

    @Override
    public void stop() {
        if (worker != null) {
            consumer.wakeup();
            try {
                worker.join();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        } else {
            consumer.close(Duration.ofSeconds(1));
            producer.close(Duration.ofSeconds(1));
        }
    }
}
