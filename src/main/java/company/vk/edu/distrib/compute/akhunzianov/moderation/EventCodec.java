package company.vk.edu.distrib.compute.akhunzianov.moderation;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

public final class EventCodec {

    private static final String CREATED = "created-v1";
    private static final String MODERATED = "moderated-v1";
    private static final int CREATED_FIELDS = 4;
    private static final int MODERATED_FIELDS = 5;

    private EventCodec() {
    }

    public static String encode(LinkCreatedEvent event) {
        return String.join("\t", CREATED, encodeText(event.id()),
            encodeText(event.url()), event.createdAt().toString());
    }

    public static String encode(ModerationResultEvent event) {
        return String.join("\t", MODERATED, encodeText(event.id()), event.passed().toString(),
            encodeText(event.reason()), event.moderatedAt().toString());
    }

    public static LinkCreatedEvent created(String value) {
        var fields = fields(value, CREATED, CREATED_FIELDS);
        var id = decodeText(fields[1]);
        var url = decodeText(fields[2]);
        if (id.isBlank() || url.isBlank()) {
            throw new IllegalArgumentException("Empty link id or URL");
        }
        return new LinkCreatedEvent(id, url, Instant.parse(fields[3]));
    }

    public static ModerationResultEvent moderated(String value) {
        var fields = fields(value, MODERATED, MODERATED_FIELDS);
        if (!"true".equals(fields[2]) && !"false".equals(fields[2])) {
            throw new IllegalArgumentException("Invalid moderation decision");
        }
        var id = decodeText(fields[1]);
        if (id.isBlank()) {
            throw new IllegalArgumentException("Empty link id");
        }
        return new ModerationResultEvent(id, Boolean.valueOf(fields[2]), decodeText(fields[3]),
            Instant.parse(fields[4]));
    }

    private static String[] fields(String value, String type, int count) {
        var fields = value.split("\t", -1);
        if (fields.length != count || !type.equals(fields[0])) {
            throw new IllegalArgumentException("Invalid " + type + " event");
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
