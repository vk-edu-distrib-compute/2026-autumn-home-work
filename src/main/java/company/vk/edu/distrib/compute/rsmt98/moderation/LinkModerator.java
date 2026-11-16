package company.vk.edu.distrib.compute.rsmt98.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

final class LinkModerator {
    private final List<String> bannedWords;

    LinkModerator(List<String> bannedWords) {
        this.bannedWords = bannedWords;
    }

    ProducerRecord<byte[], byte[]> output(ConsumerRecord<byte[], byte[]> record) {
        try {
            ModerationResultEvent result = moderate(record.key(), record.value());
            return new ProducerRecord<>(
                    ModerationEventUtils.MODERATED_TOPIC,
                    record.key(),
                    ModerationEventUtils.serializeResult(result));
        } catch (IllegalArgumentException e) {
            Headers headers = new RecordHeaders(record.headers());
            String message = e.getMessage();
            headers.add(
                    "error",
                    (message == null || message.isEmpty() ? e.getClass().getSimpleName() : message)
                            .getBytes(StandardCharsets.UTF_8));
            return new ProducerRecord<>(
                    ModerationEventUtils.DEAD_LETTER_TOPIC,
                    null,
                    null,
                    record.key(),
                    record.value(),
                    headers);
        }
    }

    private ModerationResultEvent moderate(byte @Nullable [] key, byte @Nullable [] value) {
        if (value == null) {
            throw new IllegalArgumentException("Missing event value");
        }
        LinkCreatedEvent event = ModerationEventUtils.deserializeCreated(value);
        if (!Arrays.equals(key, event.id().getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Event key does not match link ID");
        }
        String url = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase();
        String reason = "";
        for (String word : bannedWords) {
            if (url.contains(word)) {
                reason = "Banned word: " + word;
                break;
            }
        }
        return new ModerationResultEvent(event.id(), reason.isEmpty(), reason, Instant.now());
    }
}
