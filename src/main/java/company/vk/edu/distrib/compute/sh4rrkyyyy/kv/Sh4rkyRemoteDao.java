package company.vk.edu.distrib.compute.sh4rrkyyyy.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class Sh4rkyRemoteDao implements Dao<String> {
    private final HttpClient client;
    private final String url;
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;

    Sh4rkyRemoteDao(int port) {
        this.client = HttpClient.newHttpClient();
        this.url = "http://localhost:" + port;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpResponse<byte[]> rsp = send(buildReq(key).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        if (rsp.statusCode() == HTTP_NOT_FOUND) {
            throw new NoSuchElementException("no value for key: " + key);
        }
        if (rsp.statusCode() != HTTP_OK) {
            throw new IOException("unexpected response: " + rsp.statusCode());
        }
        return new String(rsp.body(), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        HttpResponse<Void> rsp = send(buildReq(key)
                        .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8)).build(),
                HttpResponse.BodyHandlers.discarding());
        if (rsp.statusCode() != HTTP_CREATED) {
            throw new IOException("unexpected response: " + rsp.statusCode());
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        HttpResponse<Void> rsp = send(buildReq(key).DELETE().build(), HttpResponse.BodyHandlers.discarding());
        if (rsp.statusCode() != HTTP_ACCEPTED) {
            throw new IOException("unexpected response: " + rsp.statusCode());
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    private HttpRequest.Builder buildReq(String key) {
        String encoded = URLEncoder.encode(key, StandardCharsets.UTF_8);
        return HttpRequest.newBuilder()
                .uri(URI.create(url + "/v0/entity?id=" + encoded));
    }

    private <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
            throws IOException {
        try {
            return client.send(request, handler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
