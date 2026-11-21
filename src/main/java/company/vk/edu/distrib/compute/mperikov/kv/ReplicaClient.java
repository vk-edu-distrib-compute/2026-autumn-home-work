package company.vk.edu.distrib.compute.mperikov.kv;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReplicaClient {
    private static final Logger log = LoggerFactory.getLogger(ReplicaClient.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    private final HttpClient client;

    ReplicaClient(HttpClient client) {
        this.client = client;
    }

    CompletableFuture<Boolean> put(int port, String id, byte[] body) {
        HttpRequest request = putRequest(port, id, body);
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(response -> succeeded(response.statusCode()))
            .exceptionally(ReplicaClient::failed);
    }

    CompletableFuture<Boolean> delete(int port, String id) {
        return client.sendAsync(deleteRequest(port, id), HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(response -> succeeded(response.statusCode()))
            .exceptionally(ReplicaClient::failed);
    }

    void readInto(ReadVote vote, int port, String id) {
        client.sendAsync(getRequest(port, id), HttpResponse.BodyHandlers.ofByteArray())
            .whenComplete((response, error) -> remember(vote, response, error));
    }

    static void miss(@Nullable Throwable error) {
        if (error != null && log.isDebugEnabled()) {
            log.debug("Replica was not reached", error);
        }
    }

    private static void remember(ReadVote vote, @Nullable HttpResponse<byte[]> response, @Nullable Throwable error) {
        if (error != null || response == null) {
            miss(error);
            vote.failed();
            return;
        }
        vote.remember(response.statusCode(), response.body());
    }

    private static boolean succeeded(int status) {
        return status >= KvCodes.OK && status < KvCodes.BAD_REQUEST;
    }

    private static boolean failed(Throwable error) {
        miss(error);
        return false;
    }

    private static HttpRequest putRequest(int port, String id, byte[] body) {
        return HttpRequest.newBuilder(entityUri(port, id))
            .timeout(TIMEOUT)
            .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
            .build();
    }

    private static HttpRequest getRequest(int port, String id) {
        return HttpRequest.newBuilder(entityUri(port, id))
            .timeout(TIMEOUT)
            .GET()
            .build();
    }

    private static HttpRequest deleteRequest(int port, String id) {
        return HttpRequest.newBuilder(entityUri(port, id))
            .timeout(TIMEOUT)
            .DELETE()
            .build();
    }

    private static URI entityUri(int port, String id) {
        String encoded = URLEncoder.encode(id, StandardCharsets.UTF_8);
        return URI.create("http://localhost:" + port + "/v0/entity?id=" + encoded + "&stored=1");
    }
}
