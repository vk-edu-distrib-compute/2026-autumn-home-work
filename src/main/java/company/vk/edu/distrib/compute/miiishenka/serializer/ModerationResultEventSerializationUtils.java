package company.vk.edu.distrib.compute.miiishenka.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;

public final class ModerationResultEventSerializationUtils {
    public static byte[] serialize(ModerationResultEvent event) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeUTF(event.id());
            output.writeBoolean(event.passed());
            output.writeUTF(event.reason());
            output.writeLong(event.moderatedAt().toEpochMilli());
        }
        return bytes.toByteArray();
    }

    public static ModerationResultEvent deserialize(byte[] data) throws IOException {
        try (
                ByteArrayInputStream bytes = new ByteArrayInputStream(data);
                DataInputStream input = new DataInputStream(bytes)
        ) {
            String id = input.readUTF();
            boolean passed = input.readBoolean();
            String reason = input.readUTF();
            Instant moderatedAt = Instant.ofEpochMilli(input.readLong());

            if (input.available() != 0) {
                throw new IOException("Unexpected data after ModerationResultEvent");
            }

            return new ModerationResultEvent(
                    id,
                    passed,
                    reason,
                    moderatedAt
            );
        }
    }

    private ModerationResultEventSerializationUtils() {
    }
}
