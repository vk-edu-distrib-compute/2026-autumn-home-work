package company.vk.edu.distrib.compute.netheer.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Pattern;

final class ModerationEvents {
    private static final Pattern LINK_ID = Pattern.compile("[A-Za-z0-9]{10}");

    private ModerationEvents() {
    }

    static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    static byte[] encodeCreated(LinkCreatedEvent event) {
        return bytes(event.createdAt() + "\n" + event.url());
    }

    static byte[] encodeResult(ModerationResultEvent event) {
        return bytes(event.moderatedAt() + "\n" + event.passed() + "\n" + event.reason());
    }

    static LinkCreatedEvent decodeCreated(byte @Nullable [] key, byte @Nullable [] value)
            throws CharacterCodingException {
        String[] fields = text(value).split("\n", 2);
        if (fields.length != 2 || fields[1].isBlank()) {
            throw new IllegalArgumentException("Expected a creation timestamp and a URL");
        }
        return new LinkCreatedEvent(linkId(key), fields[1], Instant.parse(fields[0]));
    }

    static ModerationResultEvent decodeResult(byte @Nullable [] key, byte @Nullable [] value)
            throws CharacterCodingException {
        String[] fields = text(value).split("\n", 3);
        if (fields.length != 3 || !("true".equals(fields[1]) || "false".equals(fields[1]))) {
            throw new IllegalArgumentException("Expected a timestamp, a decision and a reason");
        }
        return new ModerationResultEvent(linkId(key), Boolean.valueOf(fields[1]), fields[2], Instant.parse(fields[0]));
    }

    private static String linkId(byte @Nullable [] key) throws CharacterCodingException {
        String id = text(key);
        if (!LINK_ID.matcher(id).matches()) {
            throw new IllegalArgumentException("Invalid link ID");
        }
        return id;
    }

    private static String text(byte @Nullable [] data) throws CharacterCodingException {
        if (data == null) {
            throw new IllegalArgumentException("Missing message key or value");
        }
        return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(data)).toString();
    }
}
