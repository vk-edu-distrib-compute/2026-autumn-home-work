package company.vk.edu.distrib.compute.mperikov.kv;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;

final class KvCodes {
    static final String GET = "GET";
    static final String PUT = "PUT";
    static final String DELETE = "DELETE";
    static final String STATUS_PATH = "/v0/status";
    static final String ENTITY_PATH = "/v0/entity";
    static final String ENTITY_PREFIX = "/v0/entity/";
    static final String ID = "id";
    static final int OK = 200;
    static final int CREATED = 201;
    static final int ACCEPTED = 202;
    static final int BAD_REQUEST = 400;
    static final int NOT_FOUND = 404;
    static final int SERVER_ERROR = 500;
    static final int UNAVAILABLE = 503;
    static final String ACK = "ack";
    static final String STORED = "stored";
    static final String CLUSTER = "CLUSTER";

    private KvCodes() {
    }

    static boolean isStatus(String method, String path) {
        return GET.equals(method) && STATUS_PATH.equals(path);
    }

    static boolean isEntity(String path) {
        return ENTITY_PATH.equals(path) || path.startsWith(ENTITY_PREFIX);
    }

    static String pathId(String path) {
        if (!path.startsWith(ENTITY_PREFIX)) {
            return "";
        }
        return decode(path.substring(ENTITY_PREFIX.length()));
    }

    static String entityId(@Nullable String query) {
        return queryValue(query, ID);
    }

    static boolean storedLocally(@Nullable String query) {
        return "1".equals(queryValue(query, STORED));
    }

    static int requiredAck(@Nullable String query, int replicas) {
        String raw = queryValue(query, ACK);
        if (raw.isEmpty()) {
            return 1;
        }
        return parsedAck(raw, replicas);
    }

    static int replicaTotal(int... members) {
        if (members.length == 0) {
            return 1;
        }
        return members.length;
    }

    static int[] memberPorts() {
        String raw = System.getProperty(CLUSTER);
        if (raw == null || raw.isBlank()) {
            return new int[0];
        }
        return parsePorts(raw);
    }

    static int[] replicaPorts(String key, int... ports) {
        int size = ports.length;
        int start = Math.floorMod(key.hashCode(), size);
        int[] replicas = new int[size];
        for (int offset = 0; offset < size; offset++) {
            replicas[offset] = ports[(start + offset) % size];
        }
        return replicas;
    }

    private static int parsedAck(String raw, int replicas) {
        try {
            int ack = Integer.parseInt(raw);
            if (ackOutOfRange(ack, replicas)) {
                return -1;
            }
            return ack;
        } catch (NumberFormatException ex) {
            return -1;
        }
    }

    private static boolean ackOutOfRange(int ack, int replicas) {
        return ack < 1 || ack > replicas;
    }

    private static int[] parsePorts(String raw) {
        String[] parts = raw.split(",");
        int[] ports = new int[parts.length];
        for (int index = 0; index < parts.length; index++) {
            ports[index] = Integer.parseInt(parts[index].trim());
        }
        return ports;
    }

    private static String queryValue(@Nullable String query, String name) {
        if (query == null || query.isEmpty()) {
            return "";
        }
        String prefix = name + "=";
        for (String part : query.split("&")) {
            if (part.startsWith(prefix)) {
                return decode(part.substring(prefix.length()));
            }
        }
        return "";
    }

    private static String decode(String raw) {
        try {
            return URLDecoder.decode(raw, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            return "";
        }
    }
}
