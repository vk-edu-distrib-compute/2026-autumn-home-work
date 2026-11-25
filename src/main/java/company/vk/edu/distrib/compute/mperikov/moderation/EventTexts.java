package company.vk.edu.distrib.compute.mperikov.moderation;

import java.time.Instant;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

public final class EventTexts {
    private EventTexts() {
    }

    public static String created(LinkCreatedEvent event) {
        return event.id() + "\n" + event.createdAt().toEpochMilli() + "\n" + event.url();
    }

    public static LinkCreatedEvent createdEvent(String raw) {
        String[] lines = raw.split("\n", 3);
        if (shortCreated(lines) || lines[0].isBlank()) {
            throw new IllegalArgumentException("malformed created event");
        }
        return new LinkCreatedEvent(lines[0], lines[2], Instant.ofEpochMilli(epoch(lines[1])));
    }

    public static String moderated(ModerationResultEvent event) {
        return event.id() + "\n" + event.passed() + "\n" + event.moderatedAt().toEpochMilli() + "\n" + event.reason();
    }

    public static ModerationResultEvent moderatedEvent(String raw) {
        String[] lines = raw.split("\n", 4);
        if (shortResult(lines) || lines[0].isBlank()) {
            throw new IllegalArgumentException("malformed moderation result");
        }
        return new ModerationResultEvent(lines[0], passed(lines[1]), lines[3], Instant.ofEpochMilli(epoch(lines[2])));
    }

    private static boolean shortCreated(String... lines) {
        return lines.length < 3;
    }

    private static boolean shortResult(String... lines) {
        return lines.length < 4;
    }

    private static long epoch(String raw) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("malformed event time", ex);
        }
    }

    private static boolean passed(String raw) {
        return switch (raw) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("malformed moderation flag");
        };
    }
}
