package company.vk.edu.distrib.compute.kl1dd.kv;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public class ReplicationClient {
    private static final String INTERNAL_REQUEST_HEADER = "Internal-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";
    private static final String LOCALHOST_URL = "http://localhost:";
    private static final String ENTITY_PATH = "/v0/entity?id=";

    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_UNAVAILABLE = 503;

    private final HttpClient httpClient = HttpClient.newHttpClient();

    CompletableFuture<Boolean> deleteFromReplica(int port, String id) {
        URI uri = URI.create(LOCALHOST_URL + port + ENTITY_PATH + id);
        HttpRequest request = HttpRequest.newBuilder(uri).header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE)
                .DELETE().build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(response -> response.statusCode() == HTTP_ACCEPTED)
                .exceptionally(e -> false);
    }

    CompletableFuture<Boolean> putToReplica(int port, String id, byte[] value) {
        URI uri = URI.create(LOCALHOST_URL + port + ENTITY_PATH + id);
        HttpRequest request = HttpRequest.newBuilder(uri).header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(value)).build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(response -> response.statusCode() == HTTP_CREATED)
                .exceptionally(e -> false);
    }

    CompletableFuture<ReplicationImpl.GetReplicaResult> getFromReplica(int port, String id) {
        URI uri = URI.create(LOCALHOST_URL + port + ENTITY_PATH + id);
        HttpRequest request = HttpRequest.newBuilder(uri).header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE)
                .GET().build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> {
                    if (response.statusCode() == HTTP_OK) {
                        return new ReplicationImpl.GetReplicaResult(200, response.body());
                    }

                    if (response.statusCode() == HTTP_NOT_FOUND) {
                        return new ReplicationImpl.GetReplicaResult(404, null);
                    }

                    return new ReplicationImpl.GetReplicaResult(503, null);
                })
                .exceptionally(e -> new ReplicationImpl.GetReplicaResult(HTTP_UNAVAILABLE, null));
    }
}
