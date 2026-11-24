package company.vk.edu.distrib.compute.artyompeshkov.moderation;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.regex.Pattern;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.jspecify.annotations.Nullable;

final class EventCodec {
    private static final String FIELD_SEPARATOR = "|";
    private static final int CREATED_FIELD_COUNT = 4;
    private static final int MODERATED_FIELD_COUNT = 5;

    private EventCodec() {
    }

    static byte[] encode(LinkCreatedEvent event) {
        return String.join(FIELD_SEPARATOR, "req", event.id(), encodeBase64(event.url()),
                event.createdAt().toString()).getBytes(StandardCharsets.UTF_8);
    }

    static byte[] encode(ModerationResultEvent event) {
        return String.join(FIELD_SEPARATOR, "resp", event.id(), event.passed().toString(),
                encodeBase64(event.reason()), event.moderatedAt().toString()).getBytes(StandardCharsets.UTF_8);
    }

    static LinkCreatedEvent decodeCreated(byte @Nullable [] value, @Nullable String key) {
        String[] fields = readFields(value, key, "req", CREATED_FIELD_COUNT);
        String url = decodeBase64(fields[2]);
        URI uri = URI.create(url);
        if (uri.getHost() == null
                || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new IllegalArgumentException("Event URL must use HTTP or HTTPS and contain a host");
        }
        return new LinkCreatedEvent(fields[1], url, Instant.parse(fields[3]));
    }

    static ModerationResultEvent decodeModerated(byte @Nullable [] value, @Nullable String key) {
        String[] fields = readFields(value, key, "resp", MODERATED_FIELD_COUNT);
        if (!"true".equals(fields[2]) && !"false".equals(fields[2])) {
            throw new IllegalArgumentException("Invalid moderation verdict");
        }
        return new ModerationResultEvent(fields[1], Boolean.valueOf(fields[2]),
                decodeBase64(fields[3]), Instant.parse(fields[4]));
    }

    private static String[] readFields(byte @Nullable [] value, @Nullable String key, String type, int count) {
        if (value == null) {
            throw new IllegalArgumentException("Missing event body");
        }
        String[] fields = decodeUtf8(value).split(Pattern.quote(FIELD_SEPARATOR), -1);
        if (fields.length != count || !type.equals(fields[0]) || !fields[1].matches("[a-zA-Z0-9]{10}")) {
            throw new IllegalArgumentException("Invalid " + type + " event");
        }
        if (!fields[1].equals(key)) {
            throw new IllegalArgumentException("Event id differs from its key");
        }
        return fields;
    }

    private static String encodeBase64(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeBase64(String value) {
        return decodeUtf8(Base64.getDecoder().decode(value));
    }

    private static String decodeUtf8(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Event contains invalid UTF-8", e);
        }
    }
}
