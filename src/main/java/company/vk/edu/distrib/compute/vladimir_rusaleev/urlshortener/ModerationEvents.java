package company.vk.edu.distrib.compute.vladimir_rusaleev.urlshortener;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

final class ModerationEvents {
    static final String CREATED_TOPIC = "links.created";
    static final String MODERATED_TOPIC = "links.moderated";
    static final String DEAD_LETTER_TOPIC = "links.created.dlt";

    private static final int CREATED_FIELDS = 3;
    private static final int RESULT_FIELDS = 4;

    private ModerationEvents() {
    }

    static byte[] encodeCreated(LinkCreatedEvent event) {
        return String.join(
            "\n",
            event.id(),
            event.createdAt().toString(),
            event.url()
        ).getBytes(StandardCharsets.UTF_8);
    }

    static LinkCreatedEvent decodeCreated(String key, byte[] bytes) {
        String[] fields = text(bytes).split("\n", CREATED_FIELDS);
        if (fields.length != CREATED_FIELDS || fields[0].isEmpty() || !fields[0].equals(key)) {
            throw new IllegalArgumentException("Invalid link event");
        }
        if (fields[2].isEmpty()) {
            throw new IllegalArgumentException("Empty URL");
        }
        return new LinkCreatedEvent(fields[0], fields[2], Instant.parse(fields[1]));
    }

    static byte[] encodeResult(ModerationResultEvent event) {
        return String.join(
            "\n",
            event.id(),
            event.moderatedAt().toString(),
            event.passed().toString(),
            event.reason()
        ).getBytes(StandardCharsets.UTF_8);
    }

    static ModerationResultEvent decodeResult(String key, byte[] bytes) {
        String[] fields = text(bytes).split("\n", RESULT_FIELDS);
        if (fields.length != RESULT_FIELDS || fields[0].isEmpty() || !fields[0].equals(key)) {
            throw new IllegalArgumentException("Invalid moderation result");
        }
        if (!Boolean.TRUE.toString().equals(fields[2])
            && !Boolean.FALSE.toString().equals(fields[2])) {
            throw new IllegalArgumentException("Invalid moderation decision");
        }

        return new ModerationResultEvent(
            fields[0],
            Boolean.valueOf(fields[2]),
            fields[3],
            Instant.parse(fields[1])
        );
    }

    private static String text(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Null event");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
