package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

public class KVRemoteDao implements Dao<String> {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;

    private final HttpClient client;
    private final String entityUri;

    public KVRemoteDao(int port) {
        this.client = HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .build();
        this.entityUri = "http://localhost:" + port + "/v0/entity?id=";
    }

    @Override
    public String get(String key) throws IOException {
        HttpResponse<byte[]> response = send(request(key).GET());
        return switch (response.statusCode()) {
            case OK -> new String(response.body(), StandardCharsets.UTF_8);
            case NOT_FOUND -> throw new NoSuchElementException("No value for key: " + key);
            default -> throw unexpectedResponse(response);
        };
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8);
        HttpResponse<byte[]> response = send(request(key).PUT(body));
        if (response.statusCode() != CREATED) {
            throw unexpectedResponse(response);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        HttpResponse<byte[]> response = send(request(key).DELETE());
        if (response.statusCode() != ACCEPTED) {
            throw unexpectedResponse(response);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private HttpRequest.Builder request(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        return HttpRequest.newBuilder(URI.create(entityUri + URLEncoder.encode(key, StandardCharsets.UTF_8)))
            .timeout(TIMEOUT);
    }

    private HttpResponse<byte[]> send(HttpRequest.Builder request) throws IOException {
        try {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Request to KV service was interrupted");
            interrupted.initCause(e);
            throw interrupted;
        }
    }

    private static IOException unexpectedResponse(HttpResponse<byte[]> response) {
        if (response.statusCode() == BAD_REQUEST) {
            throw new IllegalArgumentException("KV service rejected request: " + response.uri());
        }
        return new IOException("Unexpected KV service response " + response.statusCode() + " for " + response.uri());
    }
}
