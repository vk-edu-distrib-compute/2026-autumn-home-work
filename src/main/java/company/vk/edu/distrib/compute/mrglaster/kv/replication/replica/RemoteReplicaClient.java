package company.vk.edu.distrib.compute.mrglaster.kv.replication.replica;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class RemoteReplicaClient {

    private static final String INTERNAL_HEADER = "X-Internal-Request";
    private static final String INTERNAL_VALUE = "true";
    private static final String ENTITY_PATH = "/v0/entity?id=";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);

    private final HttpClient client;

    public RemoteReplicaClient() {
        this.client = HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT).build();
    }

    public CompletableFuture<ReplicaResult> send(String method, String key, byte[] body, int targetPort) {
        HttpRequest request = buildRequest(method, key, body, targetPort);
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .handle((response, ex) -> ex == null
                        ? new ReplicaResult(response.statusCode(), response.body())
                        : ReplicaResult.UNAVAILABLE);
    }

    private HttpRequest buildRequest(String method, String key, byte[] body, int targetPort) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create("http://localhost:%d%s%s".formatted(targetPort, ENTITY_PATH, encodedKey));
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .header(INTERNAL_HEADER, INTERNAL_VALUE);
        return switch (method) {
            case "GET" -> builder.GET().build();
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            case "DELETE" -> builder.DELETE().build();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        };
    }

    public void close() {
        client.close();
    }
}
