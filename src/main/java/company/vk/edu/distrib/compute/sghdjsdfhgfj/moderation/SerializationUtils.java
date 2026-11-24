package company.vk.edu.distrib.compute.sghdjsdfhgfj.moderation;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

import java.io.*;
import java.time.Instant;
import java.util.Optional;

public final class SerializationUtils {
    private SerializationUtils() {
    }

    public static Optional<byte[]> serializeLinkCreatedEvent(LinkCreatedEvent event) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            out.writeUTF(event.id());
            out.writeUTF(event.url());
            out.writeLong(event.createdAt().toEpochMilli());
            out.flush();
            return Optional.of(bos.toByteArray());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public static Optional<byte[]> serializeModerationResultEvent(ModerationResultEvent event) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            out.writeUTF(event.id());
            out.writeBoolean(event.passed());
            out.writeUTF(event.reason());
            out.writeLong(event.moderatedAt().toEpochMilli());
            out.flush();
            return Optional.of(bos.toByteArray());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public static Optional<LinkCreatedEvent> deserializeLinkCreatedEvent(byte[] data) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
            String id = in.readUTF();
            String url = in.readUTF();
            Instant createdAt = Instant.ofEpochMilli(in.readLong());
            return Optional.of(new LinkCreatedEvent(id, url, createdAt));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public static Optional<ModerationResultEvent> deserializeModerationResultEvent(byte[] data) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
            String id = in.readUTF();
            boolean passed = in.readBoolean();
            String reason = in.readUTF();
            Instant moderatedAt = Instant.ofEpochMilli(in.readLong());
            return Optional.of(new ModerationResultEvent(id, passed, reason, moderatedAt));
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
