package company.vk.edu.distrib.compute.miiishenka.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;

public final class LinkCreatedEventSerializationUtils {
    public static byte[] serialize(LinkCreatedEvent event) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeUTF(event.id());
            output.writeUTF(event.url());
            output.writeLong(event.createdAt().toEpochMilli());
        }

        return bytes.toByteArray();
    }

    public static LinkCreatedEvent deserialize(byte[] data) throws IOException {
        try (
                ByteArrayInputStream bytes = new ByteArrayInputStream(data);
                DataInputStream input = new DataInputStream(bytes)
        ) {
            String id = input.readUTF();
            String url = input.readUTF();
            Instant createdAt = Instant.ofEpochMilli(input.readLong());
            if (input.available() != 0) {
                throw new IOException("Unexpected data after LinkCreatedEvent");
            }
            return new LinkCreatedEvent(id, url, createdAt);
        }
    }

    private LinkCreatedEventSerializationUtils() {
    }
}
