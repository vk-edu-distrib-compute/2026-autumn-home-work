package company.vk.edu.distrib.compute.iizhukov.shared.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.sun.net.httpserver.HttpExchange;

public record Request(
        String path,
        String query,
        String method,
        String body,
        byte[] rawBody,
        Map<String, String> headers
) {
    public Request {
        rawBody = rawBody.clone();
        headers = Map.copyOf(headers);
    }

    @Override
    public byte[] rawBody() {
        return rawBody.clone();
    }

    public static Request from(HttpExchange exchange) throws IOException {
        var headers = new HashMap<String, String>();
        exchange.getRequestHeaders().forEach((key, values) -> headers.put(key, values.getFirst()));
        var body = exchange.getRequestBody().readAllBytes();

        return new Request(
                exchange.getRequestURI().getPath(),
                Objects.requireNonNullElse(exchange.getRequestURI().getRawQuery(), ""),
                exchange.getRequestMethod(),
                new String(body, StandardCharsets.UTF_8),
                body,
                headers
        );
    }
}
