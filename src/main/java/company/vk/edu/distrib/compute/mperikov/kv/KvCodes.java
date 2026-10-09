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
    static final String ID = "id";
    static final int OK = 200;
    static final int CREATED = 201;
    static final int ACCEPTED = 202;
    static final int BAD_REQUEST = 400;
    static final int NOT_FOUND = 404;
    static final int SERVER_ERROR = 500;

    private KvCodes() {
    }

    static boolean isStatus(String method, String path) {
        return GET.equals(method) && STATUS_PATH.equals(path);
    }

    static boolean isEntity(String path) {
        return ENTITY_PATH.equals(path);
    }

    static String entityId(@Nullable String query) {
        String prefix = ID + "=";
        if (query == null || !query.startsWith(prefix)) {
            return "";
        }
        return decode(query.substring(prefix.length()));
    }

    private static String decode(String raw) {
        try {
            return URLDecoder.decode(raw, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            return "";
        }
    }
}
