package company.vk.edu.distrib.compute.katyadoinikova.urlshortener;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.katyadoinikova.moderation.EventCodec;
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
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class KafkaLinkModeration implements AutoCloseable {
    private static final String CREATED_TOPIC = "links.created";
    private static final String MODERATED_TOPIC = "links.moderated";
    private static final System.Logger LOG = System.getLogger(KafkaLinkModeration.class.getName());

    private final Dao<String> links;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final ThreadPoolExecutor publisher = new ThreadPoolExecutor(
            1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1024),
            Thread.ofVirtual().name("link-events-", 0).factory(),
            new ThreadPoolExecutor.AbortPolicy());
    private final AtomicBoolean running = new AtomicBoolean();
    @Nullable private Thread listener;

    KafkaLinkModeration(String bootstrapServers, Dao<String> links) {
        this.links = links;
        producer = new KafkaProducer<>(producerProperties(bootstrapServers));
        consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers));
    }

    void start() {
        consumer.subscribe(List.of(MODERATED_TOPIC));
        running.set(true);
        listener = Thread.ofVirtual().name("moderation-results").start(this::listen);
    }

    void created(String id, String url) {
        String value = EventCodec.encodeCreated(new LinkCreatedEvent(id, url, Instant.now()));
        try {
            publisher.execute(() -> {
                try {
                    producer.send(new ProducerRecord<>(CREATED_TOPIC, id, value));
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "Unable to publish link event", e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.log(System.Logger.Level.WARNING, "Unable to queue link event", e);
        }
    }

    private void listen() {
        try (KafkaConsumer<String, String> activeConsumer = consumer) {
            while (running.get()) {
                try {
                    for (ConsumerRecord<String, String> record : activeConsumer.poll(Duration.ofMillis(200))) {
                        apply(record);
                    }
                } catch (WakeupException e) {
                    if (running.get()) {
                        throw e;
                    }
                }
            }
        }
    }

    private void apply(ConsumerRecord<String, String> record) {
        try {
            ModerationResultEvent result = EventCodec.decodeResult(record.value());
            if (record.key().equals(result.id()) && !result.passed()) {
                links.delete(result.id());
            }
        } catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "Unable to apply moderation result", e);
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
        Thread currentListener = listener;
        if (currentListener != null) {
            try {
                currentListener.join(Duration.ofSeconds(2));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        publisher.shutdownNow();
        producer.close(Duration.ofMillis(200));
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return props;
    }

    private static Properties consumerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "katyadoinikova-shortener-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }
}
