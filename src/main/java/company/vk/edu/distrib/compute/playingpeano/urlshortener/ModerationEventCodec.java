package company.vk.edu.distrib.compute.playingpeano.urlshortener;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

public final class ModerationEventCodec {
    private ModerationEventCodec() {
    }

    public static String encodeCreated(LinkCreatedEvent event) {
        return encodeFields(event.id(), event.url(), Long.toString(event.createdAt().toEpochMilli()));
    }

    public static Optional<LinkCreatedEvent> tryDecodeCreated(byte @Nullable [] value) {
        if (value == null) {
            return Optional.empty();
        }
        try {
            String[] fields = decodeFields(decodeUtf8(value), 3);
            HttpUtils.validateId(fields[0]);
            HttpUtils.validateLongLink(fields[1]);
            return Optional.of(new LinkCreatedEvent(fields[0], fields[1], parseInstant(fields[2])));
        } catch (IllegalArgumentException exception) {
            return Optional.empty();
        }
    }

    public static String encodeResult(ModerationResultEvent event, String originalUrl) {
        return encodeFields(
            event.id(),
            event.passed().toString(),
            event.reason(),
            Long.toString(event.moderatedAt().toEpochMilli()),
            originalUrl
        );
    }

    static Result decodeResult(String value) {
        String[] fields = decodeFields(value, 5);
        if (!"true".equals(fields[1]) && !"false".equals(fields[1])) {
            throw new IllegalArgumentException("Invalid moderation result");
        }
        return new Result(fields[0], Boolean.parseBoolean(fields[1]), fields[2], parseInstant(fields[3]), fields[4]);
    }

    private static String encodeFields(String... fields) {
        return String.join("\n", Arrays.stream(fields)
            .map(field -> Base64.getEncoder().encodeToString(field.getBytes(StandardCharsets.UTF_8)))
            .toList());
    }

    private static String[] decodeFields(String value, int expectedCount) {
        String[] fields = value.split("\n", expectedCount + 1);
        if (fields.length != expectedCount) {
            throw new IllegalArgumentException("Invalid moderation field count");
        }
        for (int index = 0; index < fields.length; index++) {
            fields[index] = decodeUtf8(Base64.getDecoder().decode(fields[index]));
        }
        return fields;
    }

    private static Instant parseInstant(String value) {
        return Instant.ofEpochMilli(Long.parseLong(value));
    }

    private static String decodeUtf8(byte[] value) {
        String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(value, decoded.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Invalid UTF-8 moderation field");
        }
        return decoded;
    }

    record Result(String id, boolean passed, String reason, Instant moderatedAt, String originalUrl) {
    }
}
