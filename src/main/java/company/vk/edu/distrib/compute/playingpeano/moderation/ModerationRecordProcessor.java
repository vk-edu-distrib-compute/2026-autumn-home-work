package company.vk.edu.distrib.compute.playingpeano.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.playingpeano.urlshortener.ModerationEventCodec;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

final class ModerationRecordProcessor {
    private static final String LINKS_MODERATED_TOPIC = "links.moderated";
    private static final String LINKS_CREATED_DLT_TOPIC = "links.created.dlt";
    private static final String ERROR_HEADER = "error";

    private final Set<String> bannedWords;

    ModerationRecordProcessor(Set<String> bannedWords) {
        this.bannedWords = bannedWords;
    }

    ProducerRecord<byte[], byte[]> outputFor(ConsumerRecord<byte[], byte[]> record) {
        Optional<LinkCreatedEvent> decoded = ModerationEventCodec.tryDecodeCreated(record.value());
        if (decoded.isEmpty()) {
            return deadLetter(record, "Invalid link creation event");
        }

        LinkCreatedEvent event = decoded.get();
        byte[] expectedKey = event.id().getBytes(StandardCharsets.UTF_8);
        if (!Arrays.equals(expectedKey, record.key())) {
            return deadLetter(record, "Kafka key does not match the link ID");
        }

        String normalizedUrl = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
        String reason = bannedWords.stream().filter(normalizedUrl::contains).findFirst().orElse("");
        ModerationResultEvent result = new ModerationResultEvent(event.id(), reason.isEmpty(), reason, Instant.now());
        byte[] encodedResult = ModerationEventCodec.encodeResult(result, event.url()).getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(LINKS_MODERATED_TOPIC, expectedKey, encodedResult);
    }

    private static ProducerRecord<byte[], byte[]> deadLetter(ConsumerRecord<byte[], byte[]> record, String reason) {
        Headers headers = new RecordHeaders(record.headers());
        headers.add(ERROR_HEADER, reason.getBytes(StandardCharsets.UTF_8));
        return new ProducerRecord<>(LINKS_CREATED_DLT_TOPIC, null, record.key(), record.value(), headers);
    }
}
