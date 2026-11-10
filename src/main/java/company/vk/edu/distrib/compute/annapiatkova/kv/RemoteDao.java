package company.vk.edu.distrib.compute.annapiatkova.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.NoSuchElementException;

class RemoteDao implements Dao<String> {
    private final int port;
    private final HttpClient client;
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;

    RemoteDao(int... ports) throws IOException {
        port = ports[0];
        client = HttpClient.newHttpClient();
    }

    @Override
    public String get(String key) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(createURI(port, key))
            .GET()
            .build();
        HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();
        while (true) {
            try {
                HttpResponse<String> response = client.send(request, bodyHandler);
                if (response.statusCode() == HTTP_OK) {
                    return response.body();
                }
                if (response.statusCode() == HTTP_NOT_FOUND) {
                    throw new NoSuchElementException();
                }
                throw new IOException("Unexpected status code: " + response.statusCode());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for a response from the KV service", e);
            }
        }
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(createURI(port, key))
            .PUT(HttpRequest.BodyPublishers.ofString(value))
            .build();
        HttpResponse.BodyHandler<Void> bodyHandler = HttpResponse.BodyHandlers.discarding();
        while (true) {
            try {
                HttpResponse response = client.send(request, bodyHandler);
                if (response.statusCode() != HTTP_CREATED) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for a response from the KV service", e);
            }
        }
    }

    @Override
    public void delete(String key) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(createURI(port, key))
            .DELETE()
            .build();
        HttpResponse.BodyHandler<Void> bodyHandler = HttpResponse.BodyHandlers.discarding();
        while (true) {
            try {
                HttpResponse response = client.send(request, bodyHandler);
                if (response.statusCode() != HTTP_ACCEPTED) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for a response from the KV service", e);
            }
        }
    }

    @Override
    public void close() {
        //
    }

    private URI createURI(int port, String key) {
        return URI.create("http://localhost:" + port + "/v0/entity?id=" + key);
    }
}
