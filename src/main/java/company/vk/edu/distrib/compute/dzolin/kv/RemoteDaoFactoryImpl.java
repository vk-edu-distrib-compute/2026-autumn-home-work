package company.vk.edu.distrib.compute.dzolin.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

@company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest
public class RemoteDaoFactoryImpl implements company.vk.edu.distrib.compute.kv.RemoteDaoFactory<String> {
    private static final int ONE = 1;
    private static final int NOT_FOUND_CODE = 404;

    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length < ONE) {
            throw new IllegalArgumentException("expected at least one port");
        }
        return new RemoteDao(ports[0]);
    }

    private static class RemoteDao implements Dao<String> {
        private final HttpClient client = HttpClient.newHttpClient();
        private final String baseUrl;

        RemoteDao(int port) {
            baseUrl = "http://localhost:" + port + "/v0/entity?id=";
        }

        @Override
        public String get(String key) throws IOException {
            var response = send("GET", key, "");

            if (response.statusCode() == NOT_FOUND_CODE) {
                throw new NoSuchElementException("no such value for key: " + key);
            }

            checkStatus(response, 200);
            return response.body();
        }

        @Override
        public void upsert(String key, String value) throws IOException {
            var response = send("PUT", key, value);
            checkStatus(response, 201);
        }

        @Override
        public void delete(String key) throws IOException {
            var response = send("DELETE", key, "");
            checkStatus(response, 202);
        }

        private HttpResponse<String> send(String method, String key, String value) throws IOException {
            var encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
            var body = HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8);

            var request = HttpRequest.newBuilder().uri(URI.create(baseUrl + encodedKey)).method(method, body).build();

            try {
                return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("HTTP request interrupted", e);
            }
        }

        private static void checkStatus(HttpResponse<?> response, int expected) throws IOException {
            if (response.statusCode() != expected) {
                throw new IOException("unexpected HTTP status: " + response.statusCode());
            }
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }
}
