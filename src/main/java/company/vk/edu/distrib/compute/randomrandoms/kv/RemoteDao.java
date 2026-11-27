package company.vk.edu.distrib.compute.randomrandoms.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.rmi.UnexpectedException;
import java.util.NoSuchElementException;

public class RemoteDao implements Dao<String> {
    private final HttpClient client;
    private final int port;

    public RemoteDao(int port) throws IOException {
        client = HttpClient.newHttpClient();
        this.port = port;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpResponse<String> response;
        try {
            response = client
                    .send(
                            HttpRequest.newBuilder().GET().uri(URI.create(path(key))).build(),
                            HttpResponse.BodyHandlers.ofString()
                    );
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        switch (response.statusCode()) {
            case Kvs.INCORRECT_KEY_CODE -> throw new IllegalArgumentException();
            case Kvs.NOT_FOUND_CODE -> throw new NoSuchElementException();
            case Kvs.FOUND_CODE -> {
                return response.body();
            }
            default -> throw new UnexpectedException("unexpected code");
        }
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        HttpResponse<Void> response;
        try {
            response = client
                    .send(
                            HttpRequest
                                    .newBuilder()
                                    .PUT(HttpRequest.BodyPublishers.ofString(value))
                                    .uri(URI.create(path(key)))
                                    .build(),
                            HttpResponse.BodyHandlers.discarding()
                    );
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        switch (response.statusCode()) {
            case Kvs.INCORRECT_KEY_CODE -> throw new IllegalArgumentException();
            case Kvs.PUT_CODE -> {
            }
            default -> throw new UnexpectedException("unexpected code");
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        HttpResponse<Void> response;
        try {
            response = client
                    .send(
                            HttpRequest.newBuilder().DELETE().uri(URI.create(path(key))).build(),
                            HttpResponse.BodyHandlers.discarding()
                    );
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        switch (response.statusCode()) {
            case Kvs.INCORRECT_KEY_CODE -> throw new IllegalArgumentException();
            case Kvs.DELETED_CODE -> {
            }
            default -> throw new UnexpectedException("unexpected code");
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private String path(String key) {
        return String.format("http://localhost:%d%s?id=%s", port, Kvs.ENTITY, key);
    }
}
