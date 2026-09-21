package company.vk.edu.distrib.compute.iizhukov.urlshortener.api.helpers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Response {
    private final int status;
    private final String content;
    private final int length;
    private final Map<String, String> headers;

    private Response(Builder builder) {
        status = builder.status.code();
        content = builder.content;
        length = builder.content.length();
        headers = builder.headers;
    }

    public int status() {
        return status;
    }

    public String content() {
        return content;
    }

    public int length() {
        return length;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private HttpStatus status = HttpStatus.METHOD_NOT_ALLOWED;
        private String content = "";
        private final Map<String, String> headers = new ConcurrentHashMap<>(Map.of(
                "Content-Type", "text/html; charset=utf-8"
        ));

        private Builder() {

        }

        public Builder setStatus(HttpStatus status) {
            this.status = status;
            return this;
        }

        public Builder setContent(String content) {
            this.content = content;
            return this;
        }

        public Builder addHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public Response build() {
            return new Response(this);
        }
    }
}
