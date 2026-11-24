package company.vk.edu.distrib.compute.akravchenya.moderation;

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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class KafkaModerationService implements ModerationService {

    private static final String CREATED_TOPIC = "links.created";
    private static final String MODERATED_TOPIC = "links.moderated";
    private static final String DEAD_LETTER_TOPIC = "links.created.dlt";
    private static final String GROUP_ID = "akravchenya-link-moderation";
    private static final String ERROR_HEADER = "error";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final BannedWords bannedWords = new BannedWords();
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private Thread worker;
    private final AtomicBoolean running = new AtomicBoolean();

    KafkaModerationService(String bootstrapServers) {
        this.producer = new KafkaProducer<>(producerConfig(bootstrapServers));
        this.consumer = new KafkaConsumer<>(consumerConfig(bootstrapServers));
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("service is already started");
        }
        worker = new Thread(this::consume, "kafka-moderation");
        worker.start();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("service is not started");
        }
        consumer.wakeup();
        try {
            worker.join(5000);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        consumer.close(Duration.ZERO);
        producer.close(Duration.ZERO);
    }

    private void consume() {
        try {
            consumer.subscribe(List.of(CREATED_TOPIC));
            while (running.get()) {
                var records = consumer.poll(POLL_TIMEOUT);
                for (var record : records) {
                    process(record);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException expected) {
        }
    }

    private void process(ConsumerRecord<String, String> record) {
        LinkCreatedEvent event;
        try {
            event = LinkEventCodec.decodeCreated(record.value());
        } catch (RuntimeException exception) {
            sendToDeadLetter(record, exception);
            return;
        }
        var offense = bannedWords.offenseOf(event.url());
        var passed = offense.isEmpty();
        var reason = offense.map(word -> "banned word: " + word).orElse(null);
        var result = new ModerationResultEvent(event.id(), passed, reason, Instant.now());
        send(MODERATED_TOPIC, event.id(), LinkEventCodec.encodeResult(result));
    }

    private void sendToDeadLetter(ConsumerRecord<String, String> record, Exception cause) {
        send(DEAD_LETTER_TOPIC, record.key(), record.value(), errorReason(cause));
    }

    private static String errorReason(Exception cause) {
        var message = cause.getMessage();
        return message == null ? cause.getClass().getName() : cause.getClass().getSimpleName() + ": " + message;
    }

    private void send(String topic, String key, String value) {
        send(topic, key, value, null);
    }

    private void send(String topic, String key, String value, String errorHeader) {
        try {
            var record = new ProducerRecord<String, String>(topic, key, value);
            if (errorHeader != null) {
                record.headers().add(new RecordHeader(ERROR_HEADER, errorHeader.getBytes(StandardCharsets.UTF_8)));
            }
            producer.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (Exception expected) {
        }
    }

    private static Properties producerConfig(String bootstrapServers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static Properties consumerConfig(String bootstrapServers) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
}
