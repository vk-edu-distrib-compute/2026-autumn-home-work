package company.vk.edu.distrib.compute.sh4rrkyyyy.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.HttpStatus;

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

    Sh4rkyRemoteDao(int port, HttpClient client) {
        this.client = client;
        this.url = "http://localhost:" + port;
    }

    Sh4rkyRemoteDao(int port) {
       this(port, HttpClient.newHttpClient());
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        HttpResponse<byte[]> rsp = send(buildReq(key).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        if (rsp.statusCode() == HttpStatus.NOT_FOUND) {
            throw new NoSuchElementException("no value for key: " + key);
        }
        if (rsp.statusCode() != HttpStatus.OK) {
            throw new IOException("unexpected response: " + rsp.statusCode());
        }
        return new String(rsp.body(), StandardCharsets.UTF_8);
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        HttpResponse<Void> rsp = send(buildReq(key)
                        .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8)).build(),
                HttpResponse.BodyHandlers.discarding());
        if (rsp.statusCode() != HttpStatus.CREATED) {
            throw new IOException("unexpected response: " + rsp.statusCode());
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        HttpResponse<Void> rsp = send(buildReq(key).DELETE().build(), HttpResponse.BodyHandlers.discarding());
        if (rsp.statusCode() != HttpStatus.ACCEPTED) {
            throw new IOException("unexpected response: " + rsp.statusCode());
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
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
