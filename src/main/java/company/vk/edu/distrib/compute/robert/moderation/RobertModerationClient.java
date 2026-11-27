package company.vk.edu.distrib.compute.robert.moderation;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RobertModerationClient {
    private static final Logger log = LoggerFactory.getLogger(RobertModerationClient.class);

    private static final String LINKS_CREATED = "links.created";
    private static final String LINKS_MODERATED = "links.moderated";

    private static final int QUEUE_CAPACITY = 1024;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final long RETRY_DELAY_MILLIS = 200;

    private final Dao<String> dao;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;

    private final BlockingQueue<LinkCreatedEvent> pendingEvents = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread senderThread;
    private final Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean();

    public RobertModerationClient(
        String inputBootstrapServers,
        Dao<String> inputDao,
        int inputPort
    ) {
        dao = inputDao;

        consumer = new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, inputBootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "robert-shortener-" + inputPort,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic"
            ),
            new StringDeserializer(),
            new StringDeserializer()
        );

        producer = new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, inputBootstrapServers,
                ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576L,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000L,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
            ),
            new StringSerializer(),
            new StringSerializer()
        );

        senderThread = new Thread(
            this::sendPendingEvents,
            "robert-moderation-sender-" + inputPort
        );
        consumerThread = new Thread(
            this::consumeResults,
            "robert-moderation-results-" + inputPort
        );
    }

    public void start() {
        running.set(true);

        senderThread.start();
        consumerThread.start();
    }

    public void stop() {
        running.set(false);

        consumer.wakeup();
        senderThread.interrupt();
        producer.close(Duration.ZERO);

        try {
            senderThread.join();
            consumerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void sendCreated(LinkCreatedEvent event) {
        if (!pendingEvents.offer(event)) {
            log.atWarn().log("Moderation queue is full, could not enqueue link {}", event.id());
        }
    }

    private void sendPendingEvents() {
        try {
            while (running.get()) {
                LinkCreatedEvent event = pendingEvents.take();

                sendCreated(createRecord(event));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ProducerRecord<String, String> createRecord(LinkCreatedEvent event) {
        return new ProducerRecord<>(
            LINKS_CREATED,
            event.id(),
            ModerationEventUtils.encodeCreated(event)
        );
    }

    private void sendCreated(ProducerRecord<String, String> record) throws InterruptedException {
        while (running.get()) {
            try {
                producer.send(record).get();
                return;
            } catch (ExecutionException | KafkaException e) {
                if (running.get()) {
                    log.atWarn().setCause(e).log("Could not send created link");
                    Thread.sleep(RETRY_DELAY_MILLIS);
                }
            }
        }
    }

    // чтобы явно вызвать consumer.close(CLOSE_TIMEOUT); в finally
    @SuppressWarnings("PMD.UseTryWithResources")
    private void consumeResults() {
        try {
            consumer.subscribe(List.of(LINKS_MODERATED));

            while (running.get()) {
                try {
                    for (var record : consumer.poll(POLL_TIMEOUT)) {
                        applyResult(record.key(), record.value());

                        if (running.get()) {
                            consumer.commitSync();
                        }
                    }
                } catch (WakeupException e) {
                    if (running.get()) {
                        throw e;
                    }
                } catch (KafkaException e) {
                    log.atWarn().setCause(e).log("Could not consume moderation results");
                    Thread.sleep(RETRY_DELAY_MILLIS);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            consumer.close(CLOSE_TIMEOUT);
        }
    }

    private void applyResult(String key, String value) throws InterruptedException {
        ModerationResultEvent result;

        try {
            result = ModerationEventUtils.decodeResult(value);
        } catch (RuntimeException e) {
            log.atWarn().setCause(e).log("Could not decode moderation result");
            return;
        }

        if (result.id().isEmpty() || !result.id().equals(key)) {
            log.atWarn().log("Invalid moderation result key {}", key);
            return;
        }

        while (running.get() && !result.passed()) {
            try {
                dao.delete(result.id());
                return;
            } catch (IllegalArgumentException e) {
                log.atWarn().setCause(e).log("Invalid moderated link {}", result.id());
                return;
            } catch (IOException e) {
                log.atWarn().setCause(e).log("Could not delete moderated link {}", result.id());
                Thread.sleep(RETRY_DELAY_MILLIS);
            }
        }
    }
}
