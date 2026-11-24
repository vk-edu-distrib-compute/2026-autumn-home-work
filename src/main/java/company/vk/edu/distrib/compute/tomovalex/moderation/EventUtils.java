package company.vk.edu.distrib.compute.tomovalex.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.tomovalex.urlshortener.LinkUtils;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

final class EventUtils {

    private EventUtils() {
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static String[] split(byte[] value, int count) {
        if (value == null) {
            throw new IllegalArgumentException("Missing event");
        }
        String[] parts = new String(value, StandardCharsets.UTF_8).split(" ", -1);
        if (parts.length != count || !LinkUtils.isValidId(parts[0])) {
            throw new IllegalArgumentException("Invalid event format");
        }
        return parts;
    }

    static byte[] serialize(LinkCreatedEvent event) {
        String value = event.id() + " " + encode(event.url()) + " " + event.createdAt();
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static byte[] serialize(ModerationResultEvent event) {
        String value = event.id() + " " + event.passed() + " " + encode(event.reason()) + " " + event.moderatedAt();
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static LinkCreatedEvent readCreated(byte[] value) {
        String[] parts = split(value, 3);
        String url = decode(parts[1]);
        if (!LinkUtils.isValidUrl(url)) {
            throw new IllegalArgumentException("Invalid link URL");
        }
        return new LinkCreatedEvent(parts[0], url, Instant.parse(parts[2]));
    }

    static ModerationResultEvent readModerated(byte[] value) {
        String[] parts = split(value, 4);
        if (!"true".equals(parts[1]) && !"false".equals(parts[1])) {
            throw new IllegalArgumentException("Invalid moderation result");
        }
        return new ModerationResultEvent(parts[0], Boolean.valueOf(parts[1]), decode(parts[2]),
            Instant.parse(parts[3]));
    }
}
