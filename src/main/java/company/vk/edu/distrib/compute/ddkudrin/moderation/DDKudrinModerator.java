package company.vk.edu.distrib.compute.ddkudrin.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DDKudrinModerator {
    private static final Logger LOG = LoggerFactory.getLogger(DDKudrinModerator.class);
    private final List<String> bannedWords;

    DDKudrinModerator() throws IOException {
        try (InputStream input = DDKudrinModerator.class.getResourceAsStream("/moderation/banned_words.txt")) {
            if (input == null) {
                throw new IOException("Moderation dictionary is missing");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                bannedWords = reader.lines().map(String::trim).filter(word -> !word.isEmpty())
                    .map(word -> word.toLowerCase(Locale.ROOT)).toList();
            }
        }
    }

    ProducerRecord<String, String> moderate(ConsumerRecord<String, String> record) {
        try {
            LinkCreatedEvent event = parseEvent(record);
            String decoded = URLDecoder.decode(event.url(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
            boolean passed = bannedWords.stream().noneMatch(decoded::contains);
            ModerationResultEvent result = new ModerationResultEvent(event.id(), passed,
                passed ? "" : "Forbidden word", Instant.now());
            return new ProducerRecord<>("links.moderated", event.id(), DDKudrinEvents.result(result, event.url()));
        } catch (RuntimeException e) {
            LOG.warn("Cannot process link event; forwarding to dead letter topic", e);
            ProducerRecord<String, String> deadLetter = new ProducerRecord<>("links.created.dlt",
                record.key(), record.value());
            String error = e.getClass().getSimpleName() + ": " + e.getMessage();
            deadLetter.headers().add("error", error.getBytes(StandardCharsets.UTF_8));
            return deadLetter;
        }
    }

    private static LinkCreatedEvent parseEvent(ConsumerRecord<String, String> record) {
        if (record.value() == null) {
            throw new IllegalArgumentException("Missing event value");
        }
        LinkCreatedEvent event = DDKudrinEvents.parseCreated(record.value());
        if (!event.id().equals(record.key())) {
            throw new IllegalArgumentException("Link id does not match message key");
        }
        return event;
    }
}
