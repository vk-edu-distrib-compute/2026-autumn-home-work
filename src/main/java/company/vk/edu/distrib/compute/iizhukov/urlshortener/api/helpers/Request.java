package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.net.httpserver.HttpExchange;

public final class Request {
    private final String path;
    private final String body;
    private final Map<String, String> headers;

    private Request(Builder builder) {
        path = builder.path;
        body = builder.body;
        headers = builder.headers;
    }

    public String path() {
        return path;
    }

    public String body() {
        return body;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public static Request from(HttpExchange exchange) throws IOException {
        var builder = new Builder()
                .setPath(exchange.getRequestURI().getPath())
                .setBody(new String(
                        exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8
                ));

        exchange.getRequestHeaders().keySet().forEach(key ->
            builder.addHeader(
                    key,
                    exchange.getRequestHeaders().get(key).getFirst()
            ));

        return builder.build();
    }

    public static final class Builder {
        private String path;
        private String body;
        private final Map<String, String> headers = new ConcurrentHashMap<>();

        private Builder() {

        }

        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public Builder setBody(String body) {
            this.body = body;
            return this;
        }

        public Builder addHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public Request build() {
            return new Request(this);
        }
    }
}
