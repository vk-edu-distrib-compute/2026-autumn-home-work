package company.vk.edu.distrib.compute.nosorozhek.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Properties;

public class ModerationEventCodec {
    public String encodeCreated(LinkCreatedEvent event) {
        Properties props = new Properties();
        props.setProperty("id", event.id());
        props.setProperty("url", event.url());
        props.setProperty("createdAt", event.createdAt().toString());

        try (StringWriter writer = new StringWriter()) {
            props.store(writer, null);
            return writer.toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize event", e);
        }
    }

    public LinkCreatedEvent decodeCreated(String value) {
        Properties props = new Properties();
        try (StringReader reader = new StringReader(value)) {
            props.load(reader);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize event", e);
        }

        String id = props.getProperty("id");
        String url = props.getProperty("url");
        String createdAt = props.getProperty("createdAt");
        if (id == null || url == null || createdAt == null) {
            throw new IllegalArgumentException("Missing required fields in event data");
        }

        return new LinkCreatedEvent(
                id,
                url,
                Instant.parse(createdAt)
        );
    }

    public String encodeResult(ModerationResultEvent event) {
        Properties props = new Properties();
        props.setProperty("id", event.id());
        props.setProperty("passed", event.passed().toString());
        if (event.reason() != null) {
            props.setProperty("reason", event.reason());
        }
        props.setProperty("moderatedAt", event.moderatedAt().toString());

        try (StringWriter writer = new StringWriter()) {
            props.store(writer, null);
            return writer.toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize event", e);
        }
    }

    public ModerationResultEvent decodeResult(String value) {
        Properties props = new Properties();
        try (StringReader reader = new StringReader(value)) {
            props.load(reader);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize event", e);
        }

        String id = props.getProperty("id");
        String passed = props.getProperty("passed");
        String moderatedAt = props.getProperty("moderatedAt");
        if (id == null || passed == null || moderatedAt == null) {
            throw new IllegalArgumentException("Missing required fields in event data");
        }

        return new ModerationResultEvent(
                id,
                Boolean.valueOf(passed),
                props.getProperty("reason"),
                Instant.parse(moderatedAt)
        );
    }
}
