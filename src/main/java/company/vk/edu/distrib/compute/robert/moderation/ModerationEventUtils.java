package company.vk.edu.distrib.compute.robert.moderation;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

public final class ModerationEventUtils {
    private static final int CREATED_FIELDS_COUNT = 3;
    private static final int RESULT_FIELDS_COUNT = 4;

    private ModerationEventUtils() {
    }

    public static String encodeCreated(LinkCreatedEvent event) {
        return String.join(
            "\t",
            encode(event.id()),
            encode(event.url()),
            event.createdAt().toString()
        );
    }

    public static LinkCreatedEvent decodeCreated(String value) {
        String[] fields = split(value, CREATED_FIELDS_COUNT);

        return new LinkCreatedEvent(
            decode(fields[0]),
            decode(fields[1]),
            Instant.parse(fields[2])
        );
    }

    public static String encodeResult(ModerationResultEvent event) {
        return String.join(
            "\t",
            encode(event.id()),
            event.passed().toString(),
            encode(event.reason()),
            event.moderatedAt().toString()
        );
    }

    public static ModerationResultEvent decodeResult(String value) {
        String[] fields = split(value, RESULT_FIELDS_COUNT);

        if (!"true".equals(fields[1]) && !"false".equals(fields[1])) {
            throw new IllegalArgumentException();
        }

        return new ModerationResultEvent(
            decode(fields[0]),
            Boolean.valueOf(fields[1]),
            decode(fields[2]),
            Instant.parse(fields[3])
        );
    }

    private static String[] split(String value, int fieldsCount) {
        if (value == null) {
            throw new IllegalArgumentException();
        }

        String[] fields = value.split("\t", -1);
        if (fields.length != fieldsCount) {
            throw new IllegalArgumentException();
        }

        return fields;
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
