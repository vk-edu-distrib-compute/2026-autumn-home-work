package company.vk.edu.distrib.compute.dzolin.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.dzolin.moderation.ModerationEvents;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class LinkModeration {
    private static final Logger LOG = LoggerFactory.getLogger(LinkModeration.class);
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final Dao<String> linkDao;
    private final Thread worker;

    LinkModeration(String bootstrapServers, Dao<String> linkDao) {
        this.linkDao = linkDao;

        producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000
        ));
        consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, "dzolin-shortener-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));

        worker = Thread.ofVirtual().name("dzolin-moderation-results").start(this::consume);
    }

    void created(String id, String url) {
        LinkCreatedEvent event = new LinkCreatedEvent(id, url, Instant.now());
        Thread.ofVirtual().name("dzolin-link-created").start(() -> {
            try {
                producer.send(new ProducerRecord<>(ModerationEvents.CREATED_TOPIC, id,
                    ModerationEvents.encode(event)), (metadata, exception) -> {
                        if (exception != null) {
                            LOG.warn("Could not send link {} for moderation", id, exception);
                        }
                    });
            } catch (Exception exception) {
                LOG.warn("Could not queue link {} for moderation", id, exception);
            }
        });
    }

    private void consume() {
        try (consumer) {
            consumer.subscribe(List.of(ModerationEvents.MODERATED_TOPIC));
            boolean polling = true;
            while (polling) {
                try {
                    for (var record : consumer.poll(Duration.ofMillis(200))) {
                        try {
                            ModerationResultEvent result = ModerationEvents.decodeResult(record.value());
                            if (!result.passed() && result.id().equals(record.key())) {
                                linkDao.delete(result.id());
                            }
                        } catch (RuntimeException | IOException exception) {
                            LOG.warn("Could not apply moderation result", exception);
                        }
                    }
                } catch (WakeupException exception) {
                    polling = false;
                }
            }
        } catch (Exception exception) {
            LOG.error("Moderation results consumer stopped unexpectedly", exception);
        }
    }

    void stop() {
        consumer.wakeup();
        try {
            worker.join();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        producer.close(Duration.ZERO);
    }
}
