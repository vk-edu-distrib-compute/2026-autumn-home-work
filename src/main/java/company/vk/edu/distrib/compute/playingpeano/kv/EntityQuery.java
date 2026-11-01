package company.vk.edu.distrib.compute.playingpeano.kv;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;

final class EntityQuery {
    private static final String ID_PARAMETER = "id";
    private static final String ACK_PARAMETER = "ack";
    private static final char PARAMETER_SEPARATOR = '=';
    private static final int PARAMETER_SEPARATOR_NOT_FOUND = -1;
    private static final int DEFAULT_ACK = 1;
    private static final int MIN_ACK = 1;

    private final String key;
    private final @Nullable String ackRaw;

    private EntityQuery(String key, @Nullable String ackRaw) {
        this.key = key;
        this.ackRaw = ackRaw;
    }

    String key() {
        return key;
    }

    int ack(int replicationFactor) {
        int value = ackRaw == null ? DEFAULT_ACK : Integer.parseInt(ackRaw);
        if (value < MIN_ACK || value > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack");
        }
        return value;
    }

    static EntityQuery parse(@Nullable String rawQuery) {
        if (rawQuery == null) {
            throw new IllegalArgumentException("Missing id");
        }
        return queryFor(collect(rawQuery));
    }

    private static Fields collect(String rawQuery) {
        Fields fields = new Fields();
        for (String parameter : rawQuery.split("&")) {
            @Nullable Assignment assignment = assignment(parameter);
            if (assignment != null) {
                fields.accept(assignment);
            }
        }
        return fields;
    }

    private static @Nullable Assignment assignment(String parameter) {
        int separator = parameter.indexOf(PARAMETER_SEPARATOR);
        if (separator == PARAMETER_SEPARATOR_NOT_FOUND) {
            return null;
        }
        String name = parameter.substring(0, separator);
        String value = URLDecoder.decode(parameter.substring(separator + 1), StandardCharsets.UTF_8);
        return new Assignment(name, value);
    }

    private static EntityQuery queryFor(Fields fields) {
        if (fields.key == null || fields.key.isEmpty()) {
            throw new IllegalArgumentException("Missing id");
        }
        return new EntityQuery(fields.key, fields.ack);
    }

    private record Assignment(String name, String value) {
    }

    private static final class Fields {
        private @Nullable String key;
        private @Nullable String ack;

        void accept(Assignment assignment) {
            if (key == null && ID_PARAMETER.equals(assignment.name)) {
                key = assignment.value;
            } else if (ack == null && ACK_PARAMETER.equals(assignment.name)) {
                ack = assignment.value;
            }
        }
    }
}
