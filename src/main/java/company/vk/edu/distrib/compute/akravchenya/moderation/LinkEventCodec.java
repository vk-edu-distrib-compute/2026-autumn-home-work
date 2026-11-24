package company.vk.edu.distrib.compute.akravchenya.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public final class LinkEventCodec {

    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();
    private static final String SEPARATOR = " ";
    private static final int CREATED_FIELDS = 3;
    private static final int RESULT_FIELDS = 4;

    private LinkEventCodec() {
    }

    public static String encodeCreated(LinkCreatedEvent event) {
        return String.join(SEPARATOR,
            encode(event.id()),
            encode(event.url()),
            encode(event.createdAt().toString()));
    }

    public static LinkCreatedEvent decodeCreated(String line) {
        var fields = line.split(SEPARATOR);
        if (fields.length != CREATED_FIELDS) {
            throw new IllegalArgumentException("malformed link created event: " + line);
        }
        return new LinkCreatedEvent(
            decode(fields[0]),
            decode(fields[1]),
            Instant.parse(decode(fields[2])));
    }

    public static String encodeResult(ModerationResultEvent event) {
        return String.join(SEPARATOR,
            encode(event.id()),
            encode(Boolean.TRUE.equals(event.passed()) ? "1" : "0"),
            encode(event.reason() == null ? "" : event.reason()),
            encode(event.moderatedAt().toString()));
    }

    public static ModerationResultEvent decodeResult(String line) {
        var fields = line.split(SEPARATOR);
        if (fields.length != RESULT_FIELDS) {
            throw new IllegalArgumentException("malformed moderation result event: " + line);
        }
        return new ModerationResultEvent(
            decode(fields[0]),
            "1".equals(decode(fields[1])),
            decode(fields[2]),
            Instant.parse(decode(fields[3])));
    }

    private static String encode(String value) {
        return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(DECODER.decode(value), StandardCharsets.UTF_8);
    }
}
