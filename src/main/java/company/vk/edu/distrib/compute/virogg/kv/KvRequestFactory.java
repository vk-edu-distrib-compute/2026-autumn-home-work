package company.vk.edu.distrib.compute.virogg.kv;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

final class KvRequestFactory {
    private KvRequestFactory() {
    }

    static HttpRequest.Builder entity(int port, String key, Duration timeout) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        String encoded = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create("http://localhost:" + port + KvApiConstants.ENTITY_PATH
                + '?' + KvApiConstants.ID_PARAMETER + '=' + encoded);
        return HttpRequest.newBuilder(uri).timeout(timeout);
    }
}
