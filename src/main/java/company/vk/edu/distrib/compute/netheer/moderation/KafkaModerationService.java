package company.vk.edu.distrib.compute.netheer.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

final class KafkaModerationService implements ModerationService {
    private static final String GROUP_ID = "netheer-link-moderation";

    private final List<String> bannedWords;
    private final String transactionalId = GROUP_ID + "-" + UUID.randomUUID();
    private final KafkaWorker worker;

    KafkaModerationService(String bootstrapServers, List<String> bannedWords) {
        this.bannedWords = bannedWords;
        this.worker = new KafkaWorker("link-moderator", () -> moderateLinks(bootstrapServers));
    }

    @Override
    public void start() {
        worker.start();
    }

    @Override
    public void stop() {
        worker.stop();
    }

    private void moderateLinks(String bootstrapServers) {
        try (var producer = KafkaClients.producer(bootstrapServers, Map.of(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId
        ))) {
            producer.initTransactions();
            KafkaClients.consume(bootstrapServers, GROUP_ID, KafkaClients.CREATED,
                    (consumer, records) -> publishBatch(producer, consumer, records));
        }
    }

    private void publishBatch(
            KafkaProducer<byte[], byte[]> producer,
            KafkaConsumer<byte[], byte[]> consumer,
            ConsumerRecords<byte[], byte[]> records
    ) {
        producer.beginTransaction();
        try {
            for (var record : records) {
                producer.send(moderateRecord(record));
            }
            producer.sendOffsetsToTransaction(records.nextOffsets(), consumer.groupMetadata());
            producer.commitTransaction();
        } catch (KafkaException exception) {
            abortBatch(producer, exception);
            throw exception;
        }
    }

    private void abortBatch(KafkaProducer<byte[], byte[]> producer, KafkaException failure) {
        boolean interrupted = Thread.interrupted();
        try {
            producer.abortTransaction();
        } catch (KafkaException exception) {
            if (exception != failure) {
                failure.addSuppressed(exception);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private ProducerRecord<byte[], byte[]> moderateRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            LinkCreatedEvent created = ModerationEvents.decodeCreated(record.key(), record.value());
            ModerationResultEvent result = checkLink(created);
            return new ProducerRecord<>(KafkaClients.MODERATED, ModerationEvents.bytes(result.id()),
                    ModerationEvents.encodeResult(result));
        } catch (CharacterCodingException | IllegalArgumentException | DateTimeException exception) {
            Long timestamp = record.timestamp() < 0 ? null : record.timestamp();
            var deadLetter = new ProducerRecord<>(
                    KafkaClients.DEAD_LETTER, null, timestamp, record.key(), record.value(), record.headers());
            String error = exception.toString();
            deadLetter.headers().add("error", ModerationEvents.bytes(error));
            return deadLetter;
        }
    }

    private ModerationResultEvent checkLink(LinkCreatedEvent created) {
        URI uri = URI.create(created.url());
        if (uri.getHost() == null
                || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new IllegalArgumentException("Expected an HTTP or HTTPS URL");
        }
        String decodedUrl = URLDecoder.decode(created.url().replace("+", "%2B"), StandardCharsets.UTF_8)
                .toLowerCase(Locale.ROOT);
        String matchedWord = bannedWords.stream().filter(decodedUrl::contains).findFirst().orElse("");
        return new ModerationResultEvent(created.id(), matchedWord.isEmpty(), matchedWord, Instant.now());
    }
}
