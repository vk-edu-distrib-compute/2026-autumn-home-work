package company.vk.edu.distrib.compute.akhunzianov.kv;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

public class RemoteDao implements Dao<String> {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static final int OK_CODE = 200;
    private static final int CREATED_CODE = 201;
    private static final int ACCEPTED_CODE = 202;
    private static final int NOT_FOUND_CODE = 404;

    private final HttpClient client = HttpClient.newHttpClient();
    private final String base;
    private final KVService service;

    public RemoteDao(int port, KVService service) {
        this.base = "http://localhost:" + port + "/v0/entity?id=";
        this.service = service;
    }

    @Override
    public String get(String key) throws IOException {
        var response = send(request(key).GET(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() == NOT_FOUND_CODE) {
            throw new NoSuchElementException("No such key as '" + key + '\'');
        }
        expect(response, OK_CODE);
        return response.body();
    }

    @Override
    public void upsert(String key, String value) throws IOException {
        var body = HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8);
        expect(send(request(key).PUT(body), HttpResponse.BodyHandlers.discarding()), CREATED_CODE);
    }

    @Override
    public void delete(String key) throws IOException {
        expect(send(request(key).DELETE(), HttpResponse.BodyHandlers.discarding()), ACCEPTED_CODE);
    }

    @Override
    public void close() {
        client.close();
        service.stop();
    }

    private HttpRequest.Builder request(String key) {
        return HttpRequest.newBuilder(URI.create(base + URLEncoder.encode(key, StandardCharsets.UTF_8)))
            .timeout(TIMEOUT);
    }

    private <T> HttpResponse<T> send(HttpRequest.Builder request, HttpResponse.BodyHandler<T> handler)
        throws IOException {
        try {
            return client.send(request.build(), handler);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            var interrupted = new InterruptedIOException("Interrupted while talking to the KV service");
            interrupted.initCause(e);
            throw interrupted;
        }
    }

    private static void expect(HttpResponse<?> response, int code) throws IOException {
        if (response.statusCode() != code) {
            throw new IOException("KV service answered " + response.statusCode() + ", expected " + code);
        }
    }
}
