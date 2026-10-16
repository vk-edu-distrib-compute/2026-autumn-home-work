package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public final class RemoteDao implements Dao<String> {
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;

    private final int port;
    private final HttpClient client;

    public RemoteDao(int port) {
        this.port = port;
        this.client = HttpClient.newHttpClient();
    }

    @Override
    public String get(String key) throws IOException {
        validateKey(key);

        HttpRequest request = HttpRequest.newBuilder(createUri(key))
                .GET()
                .build();

        HttpResponse<byte[]> response = send(request);

        return switch (response.statusCode()) {
            case HTTP_OK -> new String(response.body(), StandardCharsets.UTF_8);
            case HTTP_BAD_REQUEST -> throw new IllegalArgumentException("Invalid key");
            case HTTP_NOT_FOUND -> throw new NoSuchElementException("Key not found: " + key);
            default -> throw unexpectedStatus(response.statusCode());
        };
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        validateKey(key);

        HttpRequest request = HttpRequest.newBuilder(createUri(key))
                .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8))
                .build();

        HttpResponse<byte[]> response = send(request);

        if (response.statusCode() == HTTP_BAD_REQUEST) {
            throw new IllegalArgumentException("Invalid key");
        }

        if (response.statusCode() != HTTP_CREATED) {
            throw unexpectedStatus(response.statusCode());
        }
    }

    @Override
    public void delete(String key) throws IOException {
        validateKey(key);

        HttpRequest request = HttpRequest.newBuilder(createUri(key))
                .DELETE()
                .build();

        HttpResponse<byte[]> response = send(request);

        if (response.statusCode() == HTTP_BAD_REQUEST) {
            throw new IllegalArgumentException("Invalid key");
        }

        if (response.statusCode() != HTTP_ACCEPTED) {
            throw unexpectedStatus(response.statusCode());
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private URI createUri(String key) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI.create(
                "http://localhost:" + port + "/v0/entity?id=" + encodedKey
        );
    }

    private HttpResponse<byte[]> send(HttpRequest request) throws IOException {
        try {
            return client.send(
                    request,
                    HttpResponse.BodyHandlers.ofByteArray()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request was interrupted", e);
        }
    }

    private static void validateKey(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key is empty");
        }
    }

    private static IOException unexpectedStatus(int statusCode) {
        return new IOException("Unexpected status code: " + statusCode);
    }
}
