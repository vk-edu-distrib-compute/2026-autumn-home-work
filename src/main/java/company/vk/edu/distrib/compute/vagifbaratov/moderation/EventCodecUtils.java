package company.vk.edu.distrib.compute.vagifbaratov.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public final class EventCodecUtils {

    private EventCodecUtils() {
    }

    public static String encode(LinkCreatedEvent event) {
        return String.join(
            "\t",
            "1",
            event.id(),
            encodeText(event.url()),
            event.createdAt().toString()
        );
    }

    public static String encode(ModerationResultEvent event) {
        return String.join(
            "\t",
            "1",
            event.id(),
            event.passed().toString(),
            encodeText(event.reason()),
            event.moderatedAt().toString()
        );
    }

    public static LinkCreatedEvent decodeCreated(String value) {
        String[] fields = fields(value, 4);
        return new LinkCreatedEvent(fields[1], decodeText(fields[2]), Instant.parse(fields[3]));
    }

    public static ModerationResultEvent decodeModerated(String value) {
        String[] fields = fields(value, 5);

        if (!"true".equals(fields[2]) && !"false".equals(fields[2])) {
            throw new IllegalArgumentException("Invalid moderation decision");
        }

        return new ModerationResultEvent(
            fields[1],
            Boolean.valueOf(fields[2]),
            decodeText(fields[3]),
            Instant.parse(fields[4])
        );
    }

    private static String[] fields(String value, int count) {
        String[] fields = value.split("\t", -1);
        if (fields.length != count || !"1".equals(fields[0]) || !fields[1].matches("[A-Za-z0-9]{10}")) {
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
