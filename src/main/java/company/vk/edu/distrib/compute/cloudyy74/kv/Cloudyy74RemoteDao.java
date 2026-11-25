package company.vk.edu.distrib.compute.cloudyy74.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

public class Cloudyy74RemoteDao implements Dao<String> {
    private static final int OK_STATUS = 200;
    private static final int CREATED_STATUS = 201;
    private static final int ACCEPTED_STATUS = 202;
    private static final int BAD_REQUEST_STATUS = 400;
    private static final int NOT_FOUND_STATUS = 404;
    private static final int UNPROCESSABLE_CONTENT_STATUS = 422;

    private final int port;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public Cloudyy74RemoteDao(int port) {
        this.port = port;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        final var request = request(key).GET().build();
        final var response = send(request);
        if (response.statusCode() == NOT_FOUND_STATUS) {
            throw new NoSuchElementException("no value for key: " + key);
        }
        checkStatus(response, OK_STATUS);
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        final var request = request(key)
                .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8))
                .build();
        checkStatus(send(request), CREATED_STATUS);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        final var request = request(key).DELETE().build();
        checkStatus(send(request), ACCEPTED_STATUS);
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private HttpRequest.Builder request(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Empty key");
        }
        final var encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        final var uri = URI.create("http://localhost:%d/v0/entity?id=%s".formatted(port, encodedKey));
        return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5));
    }

    private HttpResponse<byte[]> send(HttpRequest request) throws IOException {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while requesting KV service", e);
        }
    }

    private static void checkStatus(HttpResponse<byte[]> response, int expectedStatus) throws IOException {
        final var status = response.statusCode();
        if (status == BAD_REQUEST_STATUS || status == UNPROCESSABLE_CONTENT_STATUS) {
            throw new IllegalArgumentException("Invalid KV request");
        }
        if (status != expectedStatus) {
            throw new IOException("Unexpected KV response status: " + status);
        }
    }
}
