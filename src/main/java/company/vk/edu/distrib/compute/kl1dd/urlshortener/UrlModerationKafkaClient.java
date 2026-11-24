package company.vk.edu.distrib.compute.kl1dd.urlshortener;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

final class UrlModerationKafkaClient {
    private static final String LINKS_CREATED_TOPIC = "links.created";
    private static final String LINKS_MODERATED_TOPIC = "links.moderated";

    private final String bootstrapServers;
    private final int port;
    private final Consumer<String> onRejected;

    private KafkaProducer<String, String> kafkaProducer;
    private KafkaConsumer<String, String> kafkaConsumer;

    private final AtomicBoolean running = new AtomicBoolean();
    private final ExecutorService kafkaExecutor =
            Executors.newSingleThreadExecutor();

    UrlModerationKafkaClient(
            String bootstrapServers,
            int port,
            Consumer<String> onRejected
    ) {
        this.bootstrapServers = bootstrapServers;
        this.port = port;
        this.onRejected = onRejected;
    }

    private Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "url-shortener-" + port);
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

    private ModerationResultEvent deserializeResult(String value) {
        String[] parts = value.split("\\|", 4);

        String id = parts[0];
        boolean passed = Boolean.parseBoolean(parts[1]);
        String reason = new String(Base64.getDecoder().decode(parts[2]), StandardCharsets.UTF_8);
        Instant moderatedAt = Instant.parse(parts[3]);

        return new ModerationResultEvent(id, passed, reason, moderatedAt);
    }

    private String serializeLink(LinkCreatedEvent event) {
        String encodedUrl = Base64.getEncoder().encodeToString(event.url().getBytes(StandardCharsets.UTF_8));
        return event.id() + "|" + encodedUrl + "|" + event.createdAt();
    }

    private void runModerationLoop() {
        try (KafkaConsumer<String, String> consumer = kafkaConsumer) {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    ModerationResultEvent result = deserializeResult(record.value());

                    if (!result.passed()) {
                        onRejected.accept(result.id()); // linksDao.delete
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        }
    }

    void sendLinkCreated(String id, String linkBefore) {
        if (kafkaProducer == null) {
            return;
        }

        LinkCreatedEvent event = new LinkCreatedEvent(id, linkBefore, Instant.now());
        String value = serializeLink(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(LINKS_CREATED_TOPIC, id, value);

        kafkaExecutor.submit(() -> {
            try {
                kafkaProducer.send(record);
            } catch (RuntimeException ignored) {
                // Kafka недоступно
            }
        });
    }

    void start() {
        kafkaProducer = new KafkaProducer<>(createProducerProperties());

        kafkaConsumer = new KafkaConsumer<>(createConsumerProperties());
        kafkaConsumer.subscribe(List.of(LINKS_MODERATED_TOPIC));

        running.set(true);
        new Thread(this::runModerationLoop).start();
    }

    void stop() {
        running.set(false);
        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup();
        }

        if (kafkaProducer != null) {
            kafkaProducer.close();
        }

        kafkaExecutor.shutdownNow();
    }
}
