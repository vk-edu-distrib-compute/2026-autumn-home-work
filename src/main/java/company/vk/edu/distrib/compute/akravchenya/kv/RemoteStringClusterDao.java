package company.vk.edu.distrib.compute.akravchenya.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

/**
 * Кластерная реализация {@link Dao}, распределяющая ключи между узлами
 * KV service по rendezvous hashing.
 *
 * <p>Каждый ключ хранится ровно на одном узле; выбор узла детерминированный
 * и не зависит от порядка портов в списке.
 */
final class RemoteStringClusterDao implements Dao<String> {

    private static final String ENTITY_PATH = "/v0/entity?id=";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
    private static final int NOT_FOUND = 404;
    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final HttpClient client;
    private final int[] ports;

    RemoteStringClusterDao(int... ports) {
        this.client = HttpClient.newHttpClient();
        this.ports = ports.clone();
    }

    @Override
    public String get(String key) throws NoSuchElementException, IOException {
        var request = HttpRequest.newBuilder()
            .uri(uri(key))
            .GET()
            .timeout(REQUEST_TIMEOUT)
            .build();
        var response = send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == NOT_FOUND) {
            throw new NoSuchElementException("no value stored for key: " + key);
        }
        expect(response, 200, key);
        return response.body();
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        var request = HttpRequest.newBuilder()
            .uri(uri(key))
            .PUT(HttpRequest.BodyPublishers.ofString(value))
            .timeout(REQUEST_TIMEOUT)
            .build();
        expect(send(request, HttpResponse.BodyHandlers.discarding()), 201, key);
    }

    @Override
    public void delete(String key) throws IOException {
        var request = HttpRequest.newBuilder()
            .uri(uri(key))
            .DELETE()
            .timeout(REQUEST_TIMEOUT)
            .build();
        expect(send(request, HttpResponse.BodyHandlers.discarding()), 202, key);
    }

    @Override
    public void close() {
        client.close();
    }

    private URI uri(String key) {
        var port = ports[shardOf(key)];
        var encoded = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return URI.create("http://localhost:" + port + ENTITY_PATH + encoded);
    }

    /**
     * Выбрать узел кластера, обслуживающий заданный ключ, по rendezvous hashing.
     *
     * @param key ключ для распределения
     * @return индекс узла в списке портов
     */
    private int shardOf(String key) {
        var best = 0;
        var bestScore = score(key, 0);
        for (var index = 1; index < ports.length; index++) {
            var candidate = score(key, index);
            if (candidate > bestScore) {
                bestScore = candidate;
                best = index;
            }
        }
        return best;
    }

    private static long score(String key, int index) {
        return hash64(key + '\u0000' + index);
    }

    private static long hash64(String value) {
        var hash = FNV_OFFSET_BASIS;
        for (var i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= FNV_PRIME;
        }
        return hash;
    }

    private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) throws IOException {
        try {
            return client.send(request, handler);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while calling the KV service", exception);
        }
    }

    private <T> void expect(HttpResponse<T> response, int expected, String key) throws IOException {
        if (response.statusCode() != expected) {
            throw new IOException("unexpected status code " + response.statusCode()
                + " (expected " + expected + ") for key: " + key);
        }
    }
}
