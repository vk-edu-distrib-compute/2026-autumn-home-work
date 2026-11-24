package company.vk.edu.distrib.compute.dzolin.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.time.Instant;

public final class ModerationEvents {
    public static final String CREATED_TOPIC = "links.created";
    public static final String MODERATED_TOPIC = "links.moderated";
    public static final String DEAD_LETTER_TOPIC = "links.created.dlt";

    private ModerationEvents() {
    }

    public static String encode(LinkCreatedEvent event) {
        return event.id() + "\t" + event.url() + "\t" + event.createdAt();
    }

    public static LinkCreatedEvent decodeCreated(String value) {
        String[] parts = value.split("\t", -1);
        if (parts.length != 3 || parts[0].isBlank()) {
            throw new IllegalArgumentException("Invalid link created event");
        }
        return new LinkCreatedEvent(parts[0], parts[1], Instant.parse(parts[2]));
    }

    public static String encode(ModerationResultEvent event) {
        return event.id() + "\t" + event.passed() + "\t" + event.reason()
            + "\t" + event.moderatedAt();
    }

    public static ModerationResultEvent decodeResult(String value) {
        String[] parts = value.split("\t", -1);
        if (parts.length != 4 || parts[0].isBlank()
            || (!"true".equals(parts[1]) && !"false".equals(parts[1]))) {
            throw new IllegalArgumentException("Invalid moderation result event");
        }
        return new ModerationResultEvent(parts[0], Boolean.parseBoolean(parts[1]),
            parts[2], Instant.parse(parts[3]));
    }
}
