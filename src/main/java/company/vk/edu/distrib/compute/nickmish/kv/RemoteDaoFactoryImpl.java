package company.vk.edu.distrib.compute.nickmish.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;
import org.jspecify.annotations.NullMarked;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

@RemoteDaoFactoryTest
@NullMarked
public final class RemoteDaoFactoryImpl implements RemoteDaoFactory<String> {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private enum HttpStatus {
        OK(200),
        CREATED(201),
        ACCEPTED(202),
        NOT_FOUND(404);

        private final int code;

        HttpStatus(int code) {
            this.code = code;
        }

        int code() {
            return code;
        }
    }

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length == 0) {
            throw new IllegalArgumentException("At least one port must be provided");
        }
        int port = ports[0];
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();
        return new RemoteDao(client, port);
    }

    private record RemoteDao(HttpClient client, int port) implements Dao<String> {
        private static final String BASE_URL = "http://localhost";

        @Override
        public String get(String key) throws IOException {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + ":" + port + "/v0/entity?id=" + key))
                    .GET()
                    .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == HttpStatus.NOT_FOUND.code()) {
                    throw new NoSuchElementException("No value for key: " + key);
                }
                if (response.statusCode() != HttpStatus.OK.code()) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
                return response.body();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Request interrupted", e);
            }
        }

        @Override
        public void upsert(String key, String value) throws IOException {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + ":" + port + "/v0/entity?id=" + key))
                    .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8))
                    .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != HttpStatus.CREATED.code()) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Request interrupted", e);
            }
        }

        @Override
        public void delete(String key) throws IOException {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + ":" + port + "/v0/entity?id=" + key))
                    .DELETE()
                    .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != HttpStatus.ACCEPTED.code()) {
                    throw new IOException("Unexpected status code: " + response.statusCode());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Request interrupted", e);
            }
        }

        @Override
        public void close() {
            // HttpClient doesn't need explicit closing
        }
    }
}
