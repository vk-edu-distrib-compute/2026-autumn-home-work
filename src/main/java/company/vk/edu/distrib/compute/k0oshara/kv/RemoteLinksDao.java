package company.vk.edu.distrib.compute.k0oshara.kv;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;

final class RemoteLinksDao implements Dao<String> {
    private static final int SINGLE_NODE_COUNT = 1;

    private final HttpClient client;
    private final int[] ports;

    RemoteLinksDao(int port, HttpClient client) {
        this.ports = new int[]{port};
        this.client = client;
    }

    RemoteLinksDao(int[] ports, HttpClient client) {
        this.ports = Arrays.copyOf(ports, ports.length);
        this.client = client;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpResponse<String> response = send("GET", key, null);
        if (response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new NoSuchElementException(key);
        }
        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new IOException("Unexpected GET status: " + response.statusCode());
        }
        return response.body();
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
        HttpResponse<String> response = send("PUT", key, value);
        if (response.statusCode() != HttpURLConnection.HTTP_CREATED) {
            throw new IOException("Unexpected PUT status: " + response.statusCode());
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        HttpResponse<String> response = send("DELETE", key, null);
        if (response.statusCode() != HttpURLConnection.HTTP_ACCEPTED) {
            throw new IOException("Unexpected DELETE status: " + response.statusCode());
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private HttpResponse<String> send(String method, String key, String value) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        int port = choosePort(key);
        HttpRequest.Builder request = HttpRequest.newBuilder()
            .uri(java.net.URI.create("http://localhost:" + port + "/v0/entity?id="
                + URLEncoder.encode(key, StandardCharsets.UTF_8)));
        if ("GET".equals(method)) {
            request.GET();
        } else if ("DELETE".equals(method)) {
            request.DELETE();
        } else {
            request.PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8));
        }
        try {
            return client.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted", exception);
        }
    }

    private int choosePort(String key) {
        if (ports.length == SINGLE_NODE_COUNT) {
            return ports[0];
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            int selectedPort = ports[0];
            long selectedScore = 0;
            for (int port : ports) {
                byte[] score = digest.digest((key + ":" + port).getBytes(StandardCharsets.UTF_8));
                long value = ByteBuffer.wrap(score).getLong();
                if (Long.compareUnsigned(value, selectedScore) > 0) {
                    selectedPort = port;
                    selectedScore = value;
                }
            }
            return selectedPort;
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
