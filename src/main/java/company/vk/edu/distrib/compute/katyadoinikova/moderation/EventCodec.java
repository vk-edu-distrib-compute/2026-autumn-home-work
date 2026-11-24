package company.vk.edu.distrib.compute.katyadoinikova.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public final class EventCodec {
    private static final String TRUE = "true";
    private static final String FALSE = "false";

    private EventCodec() {
    }

    public static String encodeCreated(LinkCreatedEvent event) {
        return event.id() + '\n' + encodeText(event.url()) + '\n' + event.createdAt();
    }

    public static LinkCreatedEvent decodeCreated(String value) {
        String[] fields = fields(value, 3);
        return new LinkCreatedEvent(fields[0], decodeText(fields[1]), Instant.parse(fields[2]));
    }

    public static String encodeResult(ModerationResultEvent event) {
        return event.id() + '\n' + event.passed() + '\n'
                + encodeText(event.reason()) + '\n' + event.moderatedAt();
    }

    public static ModerationResultEvent decodeResult(String value) {
        String[] fields = fields(value, 4);
        if (!TRUE.equals(fields[1]) && !FALSE.equals(fields[1])) {
            throw new IllegalArgumentException("Invalid moderation result");
        }
        return new ModerationResultEvent(fields[0], Boolean.parseBoolean(fields[1]),
                decodeText(fields[2]), Instant.parse(fields[3]));
    }

    private static String[] fields(String value, int expected) {
        String[] fields = value.split("\n", -1);
        if (fields.length != expected || fields[0].isBlank()) {
            throw new IllegalArgumentException("Invalid event format");
        }
        return fields;
    }

    private static String encodeText(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeText(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
