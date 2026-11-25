package company.vk.edu.distrib.compute.kl1dd.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.NoSuchElementException;

public class ClusterRemoteDao implements Dao<String> {
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;

    private final int[] ports;
    private final HttpClient httpClient;

    public ClusterRemoteDao(int... ports) {
        this.httpClient = HttpClient.newHttpClient();
        this.ports = ports.clone();
    }

    private long hashingScore(String key, int port) {
        long score = Integer.toUnsignedLong((key + ":" + port).hashCode());

        for (int i = 0; i < key.length(); i++) {
            score ^= (key.charAt(i));
            score *= 53;
        }

        score ^= port;
        score *= 53;
        return score;
    }

    private int selectPort(String key) {
        int bestPort = ports[0];
        long bestScore = hashingScore(key, ports[0]);

        for (int i = 1; i < ports.length; i++) {
            long currentScore = hashingScore(key, ports[i]);

            if (Long.compareUnsigned(currentScore, bestScore) > 0) {
                bestPort = ports[i];
                bestScore = currentScore;
            }
        }

        return bestPort;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        int port = selectPort(key);

        URI uri = URI.create("http://localhost:" + port + "/v0/entity?id=" + key);
        HttpRequest request = HttpRequest.newBuilder(uri).GET().build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();

            if (statusCode == HTTP_OK) {
                return response.body();
            }

            if (statusCode == HTTP_NOT_FOUND) {
                throw new NoSuchElementException();
            }

            throw new IOException("Unexpected status code: " + statusCode);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        int port = selectPort(key);

        URI uri = URI.create("http://localhost:" + port + "/v0/entity?id=" + key);
        HttpRequest request = HttpRequest.newBuilder(uri).PUT(HttpRequest.BodyPublishers.ofString(value)).build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            if (statusCode == HTTP_CREATED) {
                return;
            }

            throw new IOException("Unexpected status code: " + statusCode);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        int port = selectPort(key);

        URI uri = URI.create("http://localhost:" + port + "/v0/entity?id=" + key);
        HttpRequest request = HttpRequest.newBuilder(uri).DELETE().build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            if (statusCode == HTTP_ACCEPTED) {
                return;
            }

            throw new IOException("Unexpected status code: " + statusCode);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        // later
    }
}
