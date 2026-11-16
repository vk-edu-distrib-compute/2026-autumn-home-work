package company.vk.edu.distrib.compute.iizhukov.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.function.ToIntFunction;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.iizhukov.shared.http.HttpStatus;

final class HttpDao implements Dao<String> {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
    private final ToIntFunction<String> portForKey;

    HttpDao(ToIntFunction<String> portForKey) {
        this.portForKey = portForKey;
    }

    @Override
    public String get(String key) throws IOException {
        var response = send(request(key).GET().build());

        if (response.statusCode() == HttpStatus.NOT_FOUND.code()) {
            throw new NoSuchElementException();
        }

        requireStatus(response, HttpStatus.OK.code());
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        var request = request(key)
                .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8))
                .build();
        requireStatus(send(request), HttpStatus.CREATED.code());
    }

    @Override
    public void delete(String key) throws IOException {
        requireStatus(send(request(key).DELETE().build()), HttpStatus.ACCEPTED.code());
    }

    private HttpRequest.Builder request(String key) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Empty key");
        }

        int port = portForKey.applyAsInt(key);
        String url = "http://localhost:" + port + "/v0/entity?id=";
        return HttpRequest.newBuilder(URI.create(url + URLEncoder.encode(key, StandardCharsets.UTF_8)))
                .timeout(REQUEST_TIMEOUT);
    }

    private HttpResponse<byte[]> send(HttpRequest request) throws IOException {
        try {
            return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static void requireStatus(HttpResponse<byte[]> response, int expected) throws IOException {
        if (response.statusCode() != expected) {
            throw new IOException("unhandled response: " + response.statusCode());
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
