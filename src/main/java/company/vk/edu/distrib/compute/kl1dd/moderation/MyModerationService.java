package company.vk.edu.distrib.compute.kl1dd.moderation;

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

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyModerationService implements ModerationService {
    private static final String LINKS_MODERATED_TOPIC = "links.moderated";
    private static final String LINKS_CREATED_DLT_TOPIC = "links.created.dlt";
    private static final String ERROR_HEADER = "error";

    private final String bootstrapServers;
    private final List<String> bannedWords;
    private KafkaConsumer<String, String> kafkaConsumer;
    private final AtomicBoolean running = new AtomicBoolean();
    private KafkaProducer<String, String> kafkaProducer;

    public MyModerationService(String bootstrapServers) throws IOException {
        this.bootstrapServers = bootstrapServers;
        this.bannedWords = loadBannedWords();
    }

    private Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "moderation-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    private Properties createProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    private void runConsumerLoop() {
        try (KafkaConsumer<String, String> consumer = kafkaConsumer) {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            LinkCreatedEvent event = deserializeLink(record.value());

            boolean passed = !isBanned(event.url());

            ModerationResultEvent result = new ModerationResultEvent(event.id(), passed,
                    passed ? "" : "banned word",
                    Instant.now());

            String value = serializeResult(result);
            ProducerRecord<String, String> recordRes = new ProducerRecord<>(LINKS_MODERATED_TOPIC,
                    result.id(),
                    value);
            kafkaProducer.send(recordRes);
        } catch (RuntimeException e) {
            ProducerRecord<String, String> deadLetter = new ProducerRecord<>(LINKS_CREATED_DLT_TOPIC,
                    record.key(),
                    record.value());

            String errorMessage = e.getMessage();
            if (errorMessage == null || errorMessage.isEmpty()) {
                errorMessage = e.getClass().getSimpleName();
            }
            deadLetter.headers().add(ERROR_HEADER, errorMessage.getBytes(StandardCharsets.UTF_8));

            kafkaProducer.send(deadLetter);
        }
    }

    private List<String> loadBannedWords() throws IOException {
        try (var stream = getClass().getClassLoader().getResourceAsStream("moderation/banned_words.txt")) {
            if (stream == null) {
                throw new IOException("banned_words.txt not found");
            }

            String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            return content.lines().toList();
        }
    }

    private boolean isBanned(String url) {
        String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        for (String bannedWord : bannedWords) {
            if (decodedUrl.contains(bannedWord.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }

        return false;
    }

    private LinkCreatedEvent deserializeLink(String value) {
        String[] parts = value.split("\\|", 3);
        String id = parts[0];
        String url = new String(Base64.getDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        Instant createdAt = Instant.parse(parts[2]);

        return new LinkCreatedEvent(id, url, createdAt);
    }

    private String serializeResult(ModerationResultEvent event) {
        String encodedReason = Base64.getEncoder().encodeToString(event.reason().getBytes(StandardCharsets.UTF_8));
        return event.id() + "|" + event.passed() + "|" + encodedReason + "|" + event.moderatedAt();
    }

    @Override
    public void start() {
        kafkaConsumer = new KafkaConsumer<>(createConsumerProperties());
        kafkaConsumer.subscribe(List.of("links.created"));

        kafkaProducer = new KafkaProducer<>(createProducerProperties());

        running.set(true);
        new Thread(this::runConsumerLoop).start();
    }

    @Override
    public void stop() {
        running.set(false);

        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup();
        }

        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }
}
