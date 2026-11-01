package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import org.jspecify.annotations.Nullable;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class QueryParams {

    private QueryParams() {
    }

    public static @Nullable String find(@Nullable String rawQuery, String name) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return null;
        }

        for (String pair : rawQuery.split("&")) {
            int separator = pair.indexOf('=');
            String pairName = separator < 0 ? pair : pair.substring(0, separator);
            if (name.equals(decode(pairName))) {
                return separator < 0 ? "" : decode(pair.substring(separator + 1));
            }
        }
        return null;
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
