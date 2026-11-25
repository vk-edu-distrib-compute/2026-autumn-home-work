package company.vk.edu.distrib.compute.akhunzianov.moderation;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

public final class ModerationServiceImpl implements ModerationService {

    private static final String GROUP = "akhunzianov-moderation-v1";

    private final ReentrantLock lock = new ReentrantLock();

    private final String bootstrapServers;
    private final UrlModerator moderator;
    @Nullable
    private KafkaProducer<String, byte[]> producer;
    @Nullable
    private KafkaWorker worker;
    private boolean started;
    private boolean stopped;

    public ModerationServiceImpl(String bootstrapServers, UrlModerator moderator) {
        this.bootstrapServers = bootstrapServers;
        this.moderator = moderator;
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (started) {
                throw new IllegalStateException("Already started");
            }
            started = true;
            var sender = new KafkaProducer<String, byte[]>(KafkaSettings.producer(bootstrapServers));
            try {
                var receiver = new KafkaWorker(bootstrapServers, GROUP, KafkaSettings.CREATED_TOPIC,
                    record -> sender.send(moderate(record)).get());
                producer = sender;
                worker = receiver;
                receiver.start();
            } catch (RuntimeException e) {
                sender.close(Duration.ZERO);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    private ProducerRecord<String, byte[]> moderate(ConsumerRecord<String, byte[]> record) {
        try {
            var event = EventCodec.created(new String(Objects.requireNonNull(record.value(), "Missing event value"),
                StandardCharsets.UTF_8));
            if (!event.id().equals(record.key())) {
                return deadLetter(record, "Message key differs from link id");
            }
            return new ProducerRecord<>(KafkaSettings.MODERATED_TOPIC, event.id(),
                EventCodec.encode(moderator.check(event)).getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            return deadLetter(record, e.toString());
        }
    }

    private static ProducerRecord<String, byte[]> deadLetter(ConsumerRecord<String, byte[]> record, String reason) {
        var deadLetter = new ProducerRecord<String, byte[]>(KafkaSettings.DEAD_LETTER_TOPIC, null,
            record.key(), record.value(), new RecordHeaders(record.headers()));
        deadLetter.headers().add("error", reason.getBytes(StandardCharsets.UTF_8));
        return deadLetter;
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            if (stopped || worker == null || producer == null) {
                throw new IllegalStateException("Not running");
            }
            worker.close();
            producer.close(Duration.ZERO);
            stopped = true;
        } finally {
            lock.unlock();
        }
    }
}
