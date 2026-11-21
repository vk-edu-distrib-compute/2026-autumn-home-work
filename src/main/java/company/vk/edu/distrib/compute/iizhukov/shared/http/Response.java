package company.vk.edu.distrib.compute.iizhukov.shared.http;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public record Response(int status, byte[] content, Map<String, String> headers) {
    public Response {
        content = content.clone();
        headers = Map.copyOf(headers);
    }

    @Override
    public byte[] content() {
        return content.clone();
    }

    public int length() {
        return content.length;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private HttpStatus status = HttpStatus.METHOD_NOT_ALLOWED;
        private byte[] content = new byte[0];
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
            return setContent(content.getBytes(StandardCharsets.UTF_8));
        }

        public Builder setContent(byte[] content) {
            this.content = content.clone();
            return this;
        }

        public Builder addHeader(String key, String value) {
            headers.put(key, value);
            return this;
        }

        public Response build() {
            return new Response(status.code(), content, headers);
        }
    }
}
