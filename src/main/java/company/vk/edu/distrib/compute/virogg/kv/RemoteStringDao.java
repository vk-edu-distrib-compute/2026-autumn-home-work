package company.vk.edu.distrib.compute.virogg.kv;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public final class RemoteStringDao implements Dao<String> {
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private final int port;
    private final HttpClient client;

    public RemoteStringDao(int port) {
        if (port < MIN_PORT || port > MAX_PORT) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        this.port = port;
        client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    }

    @Override
    public String get(String key) throws IOException {
        HttpResponse<String> response = send(KvRequestFactory.entity(port, key, TIMEOUT).GET().build());
        if (response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new NoSuchElementException(key);
        }
        requireStatus(response, HttpURLConnection.HTTP_OK);
        return response.body();
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        HttpRequest httpRequest = KvRequestFactory.entity(port, key, TIMEOUT)
                .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8)).build();
        requireStatus(send(httpRequest), HttpURLConnection.HTTP_CREATED);
    }

    @Override
    public void delete(String key) throws IOException {
        requireStatus(send(KvRequestFactory.entity(port, key, TIMEOUT).DELETE().build()),
                HttpURLConnection.HTTP_ACCEPTED);
    }

    @Override
    public void close() {
        client.close();
    }

    private HttpResponse<String> send(HttpRequest httpRequest) throws IOException {
        try {
            return client.send(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Remote DAO request interrupted", e);
        }
    }

    private static void requireStatus(HttpResponse<?> response, int expected) throws IOException {
        if (response.statusCode() != expected) {
            throw new IOException("Unexpected HTTP status: " + response.statusCode());
        }
    }
}
