package company.vk.edu.distrib.compute.akravchenya.urlshortener;

import company.vk.edu.distrib.compute.akravchenya.moderation.LinkEventCodec;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

final class KafkaModerationConnector implements AutoCloseable {

    private static final String CREATED_TOPIC = "links.created";
    private static final String MODERATED_TOPIC = "links.moderated";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService publisher;
    private final Thread worker;
    private final Consumer<String> linkRemover;
    private final AtomicBoolean running = new AtomicBoolean();

    KafkaModerationConnector(String bootstrapServers, Consumer<String> linkRemover) {
        this.linkRemover = linkRemover;
        this.producer = new KafkaProducer<>(producerConfig(bootstrapServers));
        this.consumer = new KafkaConsumer<>(consumerConfig(bootstrapServers));
        this.publisher = Executors.newSingleThreadExecutor();
        this.worker = new Thread(this::awaitResults, "kafka-moderation-results");
    }

    void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("connector is already started");
        }
        worker.start();
    }

    void notifyCreated(String id, String url, Instant createdAt) {
        publisher.execute(() -> publishCreated(id, url, createdAt));
    }

    @Override
    public void close() {
        running.set(false);
        publisher.shutdownNow();
        consumer.wakeup();
        try {
            worker.join(2000);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        consumer.close(Duration.ZERO);
        producer.close(Duration.ZERO);
    }

    private void publishCreated(String id, String url, Instant createdAt) {
        try {
            var event = new LinkCreatedEvent(id, url, createdAt);
            producer.send(new ProducerRecord<>(CREATED_TOPIC, id, LinkEventCodec.encodeCreated(event)));
        } catch (RuntimeException expected) {
        }
    }

    private void awaitResults() {
        try {
            consumer.subscribe(List.of(MODERATED_TOPIC));
            while (running.get()) {
                for (var record : consumer.poll(POLL_TIMEOUT)) {
                    handleResult(record.value());
                }
            }
        } catch (WakeupException expected) {
        }
    }

    private void handleResult(String value) {
        try {
            var event = LinkEventCodec.decodeResult(value);
            if (!event.passed()) {
                linkRemover.accept(event.id());
            }
        } catch (RuntimeException expected) {
        }
    }

    private static Properties producerConfig(String bootstrapServers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        return props;
    }

    private static Properties consumerConfig(String bootstrapServers) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "akravchenya-urlshortener-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
}
