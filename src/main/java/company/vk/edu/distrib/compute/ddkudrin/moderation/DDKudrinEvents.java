package company.vk.edu.distrib.compute.ddkudrin.moderation;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

public final class DDKudrinEvents {
    private static final int CREATED_EVENT_FIELDS = 3;

    private DDKudrinEvents() {
    }

    public static String encode(String value) {
        return Base64.getUrlEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    public static String created(LinkCreatedEvent event) {
        return encode(event.id()) + "." + encode(event.url()) + "." + encode(event.createdAt().toString());
    }

    public static LinkCreatedEvent parseCreated(String value) {
        String[] parts = value.split("\\.", -1);
        if (parts.length != CREATED_EVENT_FIELDS) {
            throw new IllegalArgumentException("Invalid link event");
        }
        String id = decode(parts[0]);
        String url = decode(parts[1]);
        if (id.isBlank() || url.isBlank()) {
            throw new IllegalArgumentException("Missing link id or URL");
        }
        return new LinkCreatedEvent(id, url, Instant.parse(decode(parts[2])));
    }

    public static String result(ModerationResultEvent result, String url) {
        return encode(result.id()) + "." + result.passed() + "." + encode(url) + "."
            + encode(result.reason()) + "." + encode(result.moderatedAt().toString());
    }
}
