package company.vk.edu.distrib.compute.sh4rrkyyyy.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

final class Sh4rkyModerationService implements ModerationService {
    private static final String GROUP = "sh4rrkyyyy-moderation";
    private static final String ERROR_HEADER = "error";

    private final String bootstrapServers;
    private final List<String> bannedWords;
    private final Thread worker = new Thread(this::run, "link-moderation");

    Sh4rkyModerationService(String bootstrapServers, List<String> bannedWords) {
        this.bootstrapServers = bootstrapServers;
        this.bannedWords = bannedWords;
    }

    @Override
    public void start() {
        worker.start();
    }

    @Override
    public void stop() {
        ModerationKafkaUtils.stop(worker);
    }

    private void run() {
        try (var producer = ModerationKafkaUtils.producer(bootstrapServers);
             var consumer = ModerationKafkaUtils.consumer(bootstrapServers, GROUP)) {
            consumer.subscribe(List.of(ModerationKafkaUtils.CREATED));
            while (!Thread.currentThread().isInterrupted()) {
                var records = consumer.poll(ModerationKafkaUtils.POLL_TIMEOUT);
                for (var record : records) {
                    producer.send(moderate(record)).get();
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (InterruptedException | InterruptException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private ProducerRecord<byte[], byte[]> moderate(ConsumerRecord<byte[], byte[]> record) {
        if (record.key() == null || record.value() == null) {
            return deadLetter(record, "Missing key or URL");
        }
        try {
            String link = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(record.value())).toString();
            var event = new LinkCreatedEvent(ModerationKafkaUtils.text(record.key()), link,
                    Instant.ofEpochMilli(record.timestamp()));
            URI uri = URI.create(event.url());
            if (uri.getScheme() == null || uri.getHost() == null) {
                return deadLetter(record, "Invalid link URL");
            }
            String url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
            String word = bannedWords.stream().filter(url::contains).findFirst().orElse("");
            String reason = word.isEmpty() ? "" : ModerationKafkaUtils.BANNED_WORD_REASON + word;
            var result = new ModerationResultEvent(event.id(), word.isEmpty(), reason, Instant.now());
            return new ProducerRecord<>(ModerationKafkaUtils.MODERATED, null, result.moderatedAt().toEpochMilli(),
                    record.key(), ModerationKafkaUtils.bytes(result.reason()));
        } catch (IllegalArgumentException | CharacterCodingException e) {
            return deadLetter(record, e.toString());
        }
    }

    private static ProducerRecord<byte[], byte[]> deadLetter(ConsumerRecord<byte[], byte[]> record, String error) {
        var forwarded = new ProducerRecord<>(ModerationKafkaUtils.CREATED_DLT, null, record.key(), record.value(),
                record.headers());
        forwarded.headers().add(ERROR_HEADER, ModerationKafkaUtils.bytes(error));
        return forwarded;
    }
}
