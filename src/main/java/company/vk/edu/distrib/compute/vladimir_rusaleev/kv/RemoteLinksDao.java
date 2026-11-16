package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

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

final class RemoteLinksDao implements Dao<String> {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final int NOT_FOUND = 404;

    private final String baseUrl;
    private final HttpClient client;

    RemoteLinksDao(int port) {
        baseUrl = "http://localhost:" + port + "/v0/entity?id=";
        client = HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    @Override
    public String get(String key) throws IOException {
        HttpResponse<byte[]> response = send(key, "GET", HttpRequest.BodyPublishers.noBody());
        if (response.statusCode() == NOT_FOUND) {
            throw new NoSuchElementException(key);
        }
        checkStatus(response, 200);
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("Null value");
        }
        HttpResponse<byte[]> response = send(key, "PUT",
            HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8));
        checkStatus(response, 201);
    }

    @Override
    public void delete(String key) throws IOException {
        HttpResponse<byte[]> response = send(key, "DELETE", HttpRequest.BodyPublishers.noBody());
        checkStatus(response, 202);
    }

    @Override
    public void close() {
        client.close();
    }

    private HttpResponse<byte[]> send(String key, String method, HttpRequest.BodyPublisher body) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Empty key");
        }
        URI uri = URI.create(baseUrl + URLEncoder.encode(key, StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(TIMEOUT)
            .header("Content-Type", "application/octet-stream")
            .method(method, body)
            .build();
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted KV request", exception);
        }
    }

    private static void checkStatus(HttpResponse<byte[]> response, int expected) throws IOException {
        if (response.statusCode() != expected) {
            throw new IOException("Unexpected KV response: " + response.statusCode());
        }
    }
}
