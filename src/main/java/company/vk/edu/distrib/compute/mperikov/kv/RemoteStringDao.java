package company.vk.edu.distrib.compute.mperikov.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

final class RemoteStringDao implements Dao<String> {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private final int port;
    private final HttpClient client = HttpClient.newHttpClient();

    RemoteStringDao(int port) {
        this.port = port;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        HttpResponse<byte[]> response = send(KvCodes.GET, key, new byte[0]);
        if (response.statusCode() == KvCodes.NOT_FOUND) {
            throw new NoSuchElementException("Key is not stored");
        }
        if (response.statusCode() != KvCodes.OK) {
            throw new IOException("KV get failed");
        }
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        checkKey(key);
        HttpResponse<byte[]> response = send(KvCodes.PUT, key, value.getBytes(StandardCharsets.UTF_8));
        if (response.statusCode() != KvCodes.CREATED) {
            throw new IOException("KV upsert failed");
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        HttpResponse<byte[]> response = send(KvCodes.DELETE, key, new byte[0]);
        if (response.statusCode() != KvCodes.ACCEPTED) {
            throw new IOException("KV delete failed");
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private HttpResponse<byte[]> send(String method, String key, byte[] body) throws IOException {
        HttpRequest.Builder request = HttpRequest.newBuilder()
            .uri(entityUri(key))
            .timeout(TIMEOUT);
        try {
            return client.send(withMethod(request, method, body).build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("KV request interrupted", ex);
        }
    }

    private static HttpRequest.Builder withMethod(HttpRequest.Builder request, String method, byte[] body) {
        if (KvCodes.GET.equals(method)) {
            return request.GET();
        }
        if (KvCodes.DELETE.equals(method)) {
            return request.DELETE();
        }
        return request.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
    }

    private URI entityUri(String key) {
        String encoded = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI.create("http://localhost:" + port + "/v0/entity?id=" + encoded);
    }

    private static void checkKey(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }
}
