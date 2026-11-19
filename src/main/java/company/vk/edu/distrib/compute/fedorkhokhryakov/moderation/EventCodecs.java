package company.vk.edu.distrib.compute.fedorkhokhryakov.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;

public final class EventCodecs {
    private static final String SEPARATOR = "\t";
    private static final int LINK_CREATED_FIELDS = 3;
    private static final int MODERATION_RESULT_FIELDS = 4;

    private EventCodecs() {
    }

    public static byte[] encode(LinkCreatedEvent event) {
        return join(event.id(), event.url(), event.createdAt().toString());
    }

    public static byte[] encode(ModerationResultEvent event) {
        return join(event.id(), event.passed().toString(), event.reason(), event.moderatedAt().toString());
    }

    public static LinkCreatedEvent decodeLinkCreated(byte @Nullable [] bytes) {
        String[] fields = split(bytes, LINK_CREATED_FIELDS);
        return new LinkCreatedEvent(fields[0], requireNotBlank(fields[1], "url"), parseInstant(fields[2]));
    }

    public static ModerationResultEvent decodeModerationResult(byte @Nullable [] bytes) {
        String[] fields = split(bytes, MODERATION_RESULT_FIELDS);
        return new ModerationResultEvent(fields[0], parseBoolean(fields[1]), fields[2], parseInstant(fields[3]));
    }

    private static byte[] join(String... fields) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                text.append(SEPARATOR);
            }
            text.append(fields[i].replaceAll("[\\t\\r\\n]", " "));
        }
        return text.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String[] split(byte @Nullable [] bytes, int expectedFields) {
        if (bytes == null) {
            throw new IllegalArgumentException("Message has no value");
        }
        String[] fields = new String(bytes, StandardCharsets.UTF_8).split(SEPARATOR, -1);
        if (fields.length != expectedFields) {
            throw new IllegalArgumentException(
                "Expected " + expectedFields + " fields but got " + fields.length);
        }
        requireNotBlank(fields[0], "id");
        return fields;
    }

    private static String requireNotBlank(String value, String name) {
        if (value.isBlank()) {
            throw new IllegalArgumentException("Field '" + name + "' is blank");
        }
        return value;
    }

    private static Instant parseInstant(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid timestamp: " + value, e);
        }
    }

    private static Boolean parseBoolean(String value) {
        if ("true".equals(value)) {
            return Boolean.TRUE;
        }
        if ("false".equals(value)) {
            return Boolean.FALSE;
        }
        throw new IllegalArgumentException("Invalid passed flag: " + value);
    }
}
