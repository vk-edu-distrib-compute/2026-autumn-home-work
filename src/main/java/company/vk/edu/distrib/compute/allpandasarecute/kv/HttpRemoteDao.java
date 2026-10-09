package company.vk.edu.distrib.compute.allpandasarecute.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

public class HttpRemoteDao implements Dao<String> {
    private static final String ENTITY_PATH = "/v0/entity?id=";

    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int NOT_FOUND = 404;

    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private static final byte[] NO_BODY = new byte[0];

    private final HttpClient client = HttpClient.newHttpClient();
    private final int[] ports;

    public HttpRemoteDao(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("At least one port is required");
        }
        this.ports = ports.clone();
    }

    @Override
    public String get(String key) throws NoSuchElementException, IOException {
        HttpResponse<byte[]> response = send(GET_METHOD, entityUri(key), NO_BODY);
        if (response.statusCode() == NOT_FOUND) {
            throw new NoSuchElementException("No value for key '%s'".formatted(key));
        }
        requireStatus(response, OK);
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        HttpResponse<byte[]> response = send(PUT_METHOD, entityUri(key), value.getBytes(StandardCharsets.UTF_8));
        requireStatus(response, CREATED);
    }

    @Override
    public void delete(String key) throws IOException {
        HttpResponse<byte[]> response = send(DELETE_METHOD, entityUri(key), NO_BODY);
        requireStatus(response, ACCEPTED);
    }

    @Override
    public void close() {
        //
    }

    private URI entityUri(String key) {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key must not be blank");
        }
        return URI.create("http://localhost:%d%s%s".formatted(ports[0], ENTITY_PATH,
            URLEncoder.encode(key, StandardCharsets.UTF_8)));
    }

    private HttpResponse<byte[]> send(String method, URI uri, byte[] body) throws IOException {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri);
        switch (method) {
            case GET_METHOD -> request.GET();
            case PUT_METHOD -> request.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            case DELETE_METHOD -> request.DELETE();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        }
        try {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request to KV service is interrupted", e);
        }
    }

    private static void requireStatus(HttpResponse<?> response, int expected) throws IOException {
        if (response.statusCode() != expected) {
            throw new IOException("Unexpected status %d, expected %d".formatted(response.statusCode(), expected));
        }
    }
}
