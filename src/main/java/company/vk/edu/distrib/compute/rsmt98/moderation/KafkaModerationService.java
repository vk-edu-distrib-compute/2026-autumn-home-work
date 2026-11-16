package company.vk.edu.distrib.compute.rsmt98.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationService;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class KafkaModerationService implements ModerationService {
    private static final String GROUP_ID = "rsmt98-link-moderation";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);

    private final String bootstrapServers;
    private final LinkModerator moderator;
    private final Lock lock = new ReentrantLock();
    private final AtomicBoolean running = new AtomicBoolean();
    private boolean startCalled;
    private boolean stopCalled;
    @Nullable private KafkaProducer<byte[], byte[]> producer;
    @Nullable private KafkaConsumer<byte[], byte[]> consumer;
    @Nullable private Thread worker;

    KafkaModerationService(String bootstrapServers, List<String> bannedWords) {
        this.bootstrapServers = bootstrapServers;
        moderator = new LinkModerator(bannedWords);
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (stopCalled) {
                throw new IllegalStateException("Moderation service is already stopped");
            }
            if (startCalled) {
                throw new IllegalStateException("Moderation service can only be started once");
            }
            startCalled = true;
            try {
                KafkaProducer<byte[], byte[]> sender = new KafkaProducer<>(producerProperties());
                producer = sender;
                KafkaConsumer<byte[], byte[]> receiver = new KafkaConsumer<>(consumerProperties());
                consumer = receiver;
                receiver.subscribe(List.of(ModerationEventUtils.CREATED_TOPIC));
                running.set(true);
                Thread thread = Thread.ofVirtual().unstarted(() -> run(receiver, sender));
                worker = thread;
                thread.start();
            } catch (RuntimeException e) {
                running.set(false);
                try {
                    closeClients(consumer, producer);
                } catch (RuntimeException ee) {
                    e.addSuppressed(ee);
                }
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (stopCalled) {
                return;
            }
            stopCalled = true;
            running.set(false);
            KafkaConsumer<byte[], byte[]> receiver = consumer;
            Thread thread = worker;
            if (thread != null) {
                if (receiver != null) {
                    receiver.wakeup();
                }
                thread.interrupt();
                try {
                    if (!thread.join(STOP_TIMEOUT)) {
                        throw new IllegalStateException("Moderation worker did not stop");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while stopping moderation", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private Map<String, Object> producerProperties() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    }

    private Map<String, Object> consumerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG,
                GROUP_ID,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false,
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                1);
    }

    private void run(KafkaConsumer<byte[], byte[]> receiver, KafkaProducer<byte[], byte[]> sender) {
        try {
            while (running.get()) {
                pollAndForward(receiver, sender);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            boolean interrupted = Thread.interrupted();
            try {
                closeClients(receiver, sender);
            } catch (RuntimeException expected) {
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void pollAndForward(
            KafkaConsumer<byte[], byte[]> receiver, KafkaProducer<byte[], byte[]> sender)
            throws InterruptedException {
        try {
            for (ConsumerRecord<byte[], byte[]> record : receiver.poll(POLL_TIMEOUT)) {
                if (!running.get()) {
                    break;
                }
                forward(receiver, sender, record);
            }
        } catch (KafkaException expected) {
        }
    }

    private void forward(
            KafkaConsumer<byte[], byte[]> receiver,
            KafkaProducer<byte[], byte[]> sender,
            ConsumerRecord<byte[], byte[]> record)
            throws InterruptedException {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        try {
            sender.send(moderator.output(record)).get();
            receiver.commitSync(
                    Map.of(partition, new OffsetAndMetadata(record.offset() + 1)), COMMIT_TIMEOUT);
        } catch (ExecutionException | KafkaException e) {
            if (running.get() && receiver.assignment().contains(partition)) {
                receiver.seek(partition, record.offset());
            }
        }
    }

    private static void closeClients(
            @Nullable KafkaConsumer<byte[], byte[]> receiver,
            @Nullable KafkaProducer<byte[], byte[]> sender) {
        RuntimeException exception = null;
        try {
            if (receiver != null) {
                receiver.close(CLOSE_TIMEOUT);
            }
        } catch (RuntimeException e) {
            exception = e;
        }
        try {
            if (sender != null) {
                sender.close(CLOSE_TIMEOUT);
            }
        } catch (RuntimeException e) {
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
