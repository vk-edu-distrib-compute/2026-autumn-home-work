package company.vk.edu.distrib.compute.ddkudrin.urlshortener;

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

import company.vk.edu.distrib.compute.ddkudrin.moderation.DDKudrinEvents;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
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

public final class DDKudrinKafkaClient {
    private static final Logger LOG = LoggerFactory.getLogger(DDKudrinKafkaClient.class);
    private static final int RESULT_FIELDS = 5;
    private static final String FAILED_RESULT = Boolean.FALSE.toString();
    private final DDKudrinLinksHandler handler;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final ThreadPoolExecutor sender = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1000));
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread worker;

    public DDKudrinKafkaClient(String bootstrapServers, DDKudrinLinksHandler handler) {
        this.handler = handler;
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        producer = new KafkaProducer<>(producerProps);
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "ddkudrin-shortener-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of("links.moderated"));
    }

    public void start() {
        running.set(true);
        worker = new Thread(this::consume, "ddkudrin-moderation-results");
        worker.start();
    }

    public void publish(String id, String url) {
        LinkCreatedEvent event = new LinkCreatedEvent(id, url, Instant.now());
        try {
            sender.execute(() -> {
                try {
                    producer.send(new ProducerRecord<>("links.created", id, DDKudrinEvents.created(event)),
                        (metadata, error) -> {
                            if (error != null) {
                                LOG.warn("Cannot publish link {} for moderation", id, error);
                            }
                        });
                } catch (RuntimeException e) {
                    LOG.warn("Cannot publish link {} for moderation", id, e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.warn("Moderation queue is full; link {} was not published", id, e);
        }
    }

    private void consume() {
        try (consumer) {
            while (running.get()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (!applyResult(record)) {
                        return;
                    }
                }
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            if (running.get()) {
                LOG.error("Moderation result consumer stopped unexpectedly", e);
            }
        } catch (RuntimeException e) {
            LOG.error("Moderation result consumer stopped unexpectedly", e);
        }
    }

    private boolean applyResult(ConsumerRecord<String, String> record) {
        String[] parts = record.value().split("\\.", -1);
        if (parts.length != RESULT_FIELDS) {
            LOG.warn("Invalid moderation result");
            return true;
        }
        try {
            String id = DDKudrinEvents.decode(parts[0]);
            if (id.equals(record.key()) && FAILED_RESULT.equals(parts[1])) {
                handler.deleteIfUnchanged(id, DDKudrinEvents.decode(parts[2]));
            }
            return true;
        } catch (IllegalArgumentException e) {
            LOG.warn("Invalid moderation result", e);
            return true;
        } catch (IOException e) {
            LOG.error("Cannot apply moderation result", e);
            return false;
        }
    }

    public void stop() {
        running.set(false);
        consumer.wakeup();
        sender.shutdownNow();
        try (producer) {
            worker.join();
            sender.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping Kafka client", e);
        }
    }
}
