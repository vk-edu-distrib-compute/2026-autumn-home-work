package company.vk.edu.distrib.compute.iizhukov.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.iizhukov.shared.http.HttpStatus;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

@RemoteDaoFactoryTest
public final class HttpRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) {
        if (ports.length == 0) {
            throw new IllegalArgumentException("Port is required");
        }

        return new HttpDao(ports[0]);
    }

    private static final class HttpDao implements Dao<String> {
        private final HttpClient client = HttpClient.newHttpClient();
        private final String url;

        private HttpDao(int port) {
            url = "http://localhost:" + port + "/v0/entity?id=";
        }

        @Override
        public String get(String key) throws IOException {
            var response = send(request(key).GET().build());

            if (response.statusCode() == HttpStatus.NOT_FOUND.code()) {
                throw new NoSuchElementException();
            }

            requireStatus(response, HttpStatus.OK.code());
            return new String(response.body(), StandardCharsets.UTF_8);
        }

        @Override
        public void upsert(String key, String value) throws IOException {
            var request = request(key)
                    .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8))
                    .build();
            requireStatus(send(request), HttpStatus.CREATED.code());
        }

        @Override
        public void delete(String key) throws IOException {
            requireStatus(send(request(key).DELETE().build()), HttpStatus.ACCEPTED.code());
        }

        private HttpRequest.Builder request(String key) {
            if (key.isEmpty()) {
                throw new IllegalArgumentException("Empty key");
            }

            return HttpRequest.newBuilder(URI.create(url + URLEncoder.encode(key, StandardCharsets.UTF_8)));
        }

        private HttpResponse<byte[]> send(HttpRequest request) throws IOException {
            try {
                return client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        private static void requireStatus(HttpResponse<byte[]> response, int expected) throws IOException {
            if (response.statusCode() != expected) {
                throw new IOException("Unexpected response status: " + response.statusCode());
            }
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
