package company.vk.edu.distrib.compute.annapiatkova.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

class ClusterRemoteDao implements Dao<String> {
    int[] ports;
    HttpClient client;
    static final int HTTP_OK = 200;
    RendezvousHashRing hashRing;

    ClusterRemoteDao(int... ports) throws IOException {
        this.ports = Arrays.copyOf(ports, ports.length);
        client = HttpClient.newHttpClient();
        List<String> clusters = new ArrayList<>(this.ports.length);
        for (int i = 0; i < this.ports.length; i++) {
            clusters.add("cluster-" + i);
        }
        hashRing = new RendezvousHashRing(clusters);
    }

    @Override
    public String get(String key) throws IOException {
        int port = ports[hashRing.getClusterIndex(key)];
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
        int port = ports[hashRing.getClusterIndex(key)];
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
        int port = ports[hashRing.getClusterIndex(key)];
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
