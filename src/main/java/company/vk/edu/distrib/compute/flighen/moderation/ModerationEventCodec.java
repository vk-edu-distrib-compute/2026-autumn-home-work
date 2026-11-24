package company.vk.edu.distrib.compute.flighen.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;

public final class ModerationEventCodec {
    private static final int CREATED_FIELDS = 3;
    private static final int RESULT_FIELDS = 4;
    private static final String TRUE_VALUE = "true";
    private static final String FALSE_VALUE = "false";

    public byte[] serializeLinkCreated(LinkCreatedEvent event) throws IOException {
        if (event.id().isBlank() || event.url().isBlank()) {
            throw new IOException("Link creation event must contain a non-empty id and URL");
        }
        return String.join("|", event.id(), encode(event.url()), event.createdAt().toString())
                .getBytes(StandardCharsets.UTF_8);
    }

    public LinkCreatedEvent deserializeLinkCreated(byte[] data) throws IOException {
        String[] fields = splitFields(data, CREATED_FIELDS);
        try {
            String url = decode(fields[1]);
            if (url.isBlank()) {
                throw new IOException("Link creation event must contain a non-empty URL");
            }
            return new LinkCreatedEvent(fields[0], url, Instant.parse(fields[2]));
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new IOException("Invalid link creation event", e);
        }
    }

    public byte[] serializeModerationResult(ModerationResultEvent event) throws IOException {
        if (event.id().isBlank()) {
            throw new IOException("Moderation result event must contain a non-empty id");
        }
        return String.join("|", event.id(), event.passed().toString(), encode(event.reason()),
                event.moderatedAt().toString()).getBytes(StandardCharsets.UTF_8);
    }

    public ModerationResultEvent deserializeModerationResult(byte[] data) throws IOException {
        String[] fields = splitFields(data, RESULT_FIELDS);
        if (!TRUE_VALUE.equals(fields[1]) && !FALSE_VALUE.equals(fields[1])) {
            throw new IOException("Moderation result must contain true or false");
        }
        try {
            return new ModerationResultEvent(fields[0], Boolean.valueOf(fields[1]), decode(fields[2]),
                    Instant.parse(fields[3]));
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new IOException("Invalid moderation result event", e);
        }
    }

    private static String[] splitFields(byte[] data, int expectedFields) throws IOException {
        if (data == null) {
            throw new IOException("Event body is missing");
        }
        String[] fields = new String(data, StandardCharsets.UTF_8).split("\\|", -1);
        if (fields.length != expectedFields || fields[0].isBlank()) {
            throw new IOException("Invalid event fields");
        }
        return fields;
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    }
}
