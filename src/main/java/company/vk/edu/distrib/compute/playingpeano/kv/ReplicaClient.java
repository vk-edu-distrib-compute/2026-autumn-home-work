package company.vk.edu.distrib.compute.playingpeano.kv;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

final class ReplicaClient implements AutoCloseable {
    static final String REPLICA_HEADER = "X-Replica-Request";
    static final String REPLICA_VALUE = "1";

    private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(200);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(800);
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_NOT_FOUND = 404;
    private static final byte[] EMPTY_VALUE = new byte[0];

    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(CONNECT_TIMEOUT)
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    CompletableFuture<GetResult> getAsync(int port, String key) {
        HttpRequest request = request(port, key).GET().build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(ReplicaClient::readGet)
            .exceptionally(exception -> GetResult.failed());
    }

    CompletableFuture<Boolean> putAsync(int port, String key, byte[] value) {
        HttpRequest request = request(port, key)
            .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
            .build();
        return sendExpected(request, STATUS_CREATED);
    }

    CompletableFuture<Boolean> deleteAsync(int port, String key) {
        HttpRequest request = request(port, key).DELETE().build();
        return sendExpected(request, STATUS_ACCEPTED);
    }

    @Override
    public void close() {
        client.close();
    }

    private CompletableFuture<Boolean> sendExpected(HttpRequest request, int expectedStatus) {
        return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
            .thenApply(response -> response.statusCode() == expectedStatus)
            .exceptionally(exception -> false);
    }

    private static HttpRequest.Builder request(int port, String key) {
        return HttpRequest.newBuilder(uriFor(port, key))
            .header(REPLICA_HEADER, REPLICA_VALUE)
            .timeout(REQUEST_TIMEOUT);
    }

    private static URI uriFor(int port, String key) {
        return URI.create(
            "http://localhost:" + port + "/v0/entity?id=" + URLEncoder.encode(key, StandardCharsets.UTF_8)
        );
    }

    private static GetResult readGet(HttpResponse<byte[]> response) {
        int status = response.statusCode();
        if (status == STATUS_OK) {
            byte[] body = response.body();
            return GetResult.found(body == null ? EMPTY_VALUE : body);
        }
        if (status == STATUS_NOT_FOUND) {
            return GetResult.missing();
        }
        return GetResult.failed();
    }

    record GetResult(boolean answered, boolean found, byte[] value) {
        static GetResult found(byte[] value) {
            return new GetResult(true, true, value);
        }

        static GetResult missing() {
            return new GetResult(true, false, EMPTY_VALUE);
        }

        static GetResult failed() {
            return new GetResult(false, false, EMPTY_VALUE);
        }
    }
}
