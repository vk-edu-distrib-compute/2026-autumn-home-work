package company.vk.edu.distrib.compute.rsmt98.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;

public final class ModerationEventUtils {
    public static final String CREATED_TOPIC = "links.created";
    public static final String MODERATED_TOPIC = "links.moderated";
    public static final String DEAD_LETTER_TOPIC = "links.created.dlt";
    private static final int CREATED_FIELDS = 3;
    private static final int RESULT_FIELDS = 4;

    private ModerationEventUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static byte[] serializeCreated(LinkCreatedEvent event) {
        return (event.id() + '\n' + event.createdAt() + '\n' + event.url())
                .getBytes(StandardCharsets.UTF_8);
    }

    public static LinkCreatedEvent deserializeCreated(byte[] value) {
        String[] fields = fields(value, CREATED_FIELDS);
        if (fields[2].isEmpty()) {
            throw new IllegalArgumentException("Missing link URL");
        }
        return new LinkCreatedEvent(fields[0], fields[2], instant(fields[1]));
    }

    public static byte[] serializeResult(ModerationResultEvent event) {
        return (event.id()
                        + '\n'
                        + event.passed()
                        + '\n'
                        + event.moderatedAt()
                        + '\n'
                        + event.reason())
                .getBytes(StandardCharsets.UTF_8);
    }

    public static ModerationResultEvent deserializeResult(byte[] value) {
        String[] fields = fields(value, RESULT_FIELDS);
        boolean passed =
                switch (fields[1]) {
                    case "true" -> true;
                    case "false" -> false;
                    default -> throw new IllegalArgumentException("Invalid moderation decision");
                };
        return new ModerationResultEvent(fields[0], passed, fields[3], instant(fields[2]));
    }

    private static String[] fields(byte[] value, int count) {
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Event is not valid UTF-8", e);
        }
        String[] fields = text.split("\n", count);
        if (fields.length != count || fields[0].isEmpty()) {
            throw new IllegalArgumentException("Invalid event fields");
        }
        return fields;
    }

    private static Instant instant(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("Invalid event timestamp", e);
        }
    }
}
