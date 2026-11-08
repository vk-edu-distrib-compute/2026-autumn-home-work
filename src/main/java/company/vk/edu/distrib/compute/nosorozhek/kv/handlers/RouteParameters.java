package company.vk.edu.distrib.compute.nosorozhek.kv.handlers;

import com.sun.net.httpserver.HttpExchange;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Matcher;

public record RouteParameters(Matcher values) {
    public static String requiredId(HttpExchange exchange) throws IllegalArgumentException {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            throw new IllegalArgumentException("Missing or empty id");
        }
        return Arrays.stream(query.split("&", -1))
                .filter(part -> part.startsWith("id="))
                .map(part -> URLDecoder.decode(part.substring(3), StandardCharsets.UTF_8))
                .filter(id -> !id.isEmpty())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Missing or empty id"));
    }

    public static Optional<Integer> optionalAck(HttpExchange exchange) throws IllegalArgumentException {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return Optional.empty();
        }
        return Arrays.stream(query.split("&", -1))
                .filter(part -> part.startsWith("ack="))
                .map(part -> URLDecoder.decode(part.substring(4), StandardCharsets.UTF_8))
                .findFirst()
                .map(ack -> {
                    if (ack.isEmpty()) {
                        throw new IllegalArgumentException("ack must be a positive integer");
                    }
                    return Integer.parseInt(ack);
                });
    }
}
