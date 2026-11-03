package company.vk.edu.distrib.compute.annapiatkova.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.NoSuchElementException;

class RemoteDao implements Dao<String> {
    int port;
    HttpClient client;
    static final int HTTP_OK = 200;

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
                throw new NoSuchElementException();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
                client.send(request, bodyHandler);
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
                client.send(request, bodyHandler);
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
