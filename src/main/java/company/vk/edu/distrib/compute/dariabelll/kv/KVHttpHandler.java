package company.vk.edu.distrib.compute.dariabelll.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class KVHttpHandler implements HttpHandler {

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private static final String STATUS_ENDPOINT = "/v0/status";

    private static final String INTERNAL_REQUEST_HEADER = "Replica-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";

    private static final int EMPTY_RESPONSE_LENGTH = -1;
    private static final byte[] EMPTY_BODY = new byte[0];

    private static final Duration REPLICA_TIMEOUT = Duration.ofMillis(500);

    private final JournaledDao dao;
    private final int port;
    private final HttpClient client;

    public KVHttpHandler(JournaledDao dao, int port, HttpClient client) {
        this.dao = dao;
        this.port = port;
        this.client = client;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            handleRequest(exchange);
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
        } catch (IOException e) {
            if (exchange.getResponseCode() != -1) {
                exchange.close();
                throw e;
            }
            sendEmptyResponse(exchange, HTTP_INTERNAL_SERVER_ERROR);
        } catch (RuntimeException e) {
            exchange.close();
            throw e;
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        List<String> allowedMethods = KVRequestUtils.getAllowedMethods(path);
        if (allowedMethods.isEmpty()) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            return;
        }
        if (!allowedMethods.contains(method)) {
            exchange.getResponseHeaders().set(
                    "Allow",
                    String.join(", ", allowedMethods)
            );
            sendEmptyResponse(exchange, HTTP_METHOD_NOT_ALLOWED);
            return;
        }
        if (STATUS_ENDPOINT.equals(path)) {
            int status = dao.isStorageAccessible() ? HTTP_OK : HTTP_SERVICE_UNAVAILABLE;
            sendEmptyResponse(exchange, status);
            return;
        }
        handleEntityRequest(exchange, method);
    }

    private void handleEntityRequest(
            HttpExchange exchange,
            String method
    ) throws IOException {
        String key = KVRequestUtils.extractKey(exchange.getRequestURI());
        if (key == null) {
            sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
            return;
        }

        String internalRequest = exchange.getRequestHeaders()
                .getFirst(INTERNAL_REQUEST_HEADER);

        if (INTERNAL_REQUEST_VALUE.equals(internalRequest)) {
            handleLocalRequest(exchange, method, key);
            return;
        }

        @Nullable ReplicationConfig config;
        try {
            config = KVRequestUtils.readReplicationConfig();
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, HTTP_SERVICE_UNAVAILABLE);
            return;
        }

        int ack;
        try {
            ack = KVRequestUtils.extractAck(exchange.getRequestURI());
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
            return;
        }

        int replicationFactor = config == null ? 1 : config.getReplicationFactor();
        if (ack < 1 || ack > replicationFactor) {
            sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
            return;
        }

        if (config == null) {
            handleLocalRequest(exchange, method, key);
            return;
        }

        int[] ports = config.getPorts();

        RendezvousHashing hashing = new RendezvousHashing(ports);
        int[] replicaIndices = hashing.selectPortIndices(key, replicationFactor);

        byte[] value = METHOD_PUT.equals(method)
                ? exchange.getRequestBody().readAllBytes()
                : EMPTY_BODY;

        List<CompletableFuture<OperationResult>> requests =
                startReplicaRequests(ports, replicaIndices, method, key, value);

        collectResults(method, ack, requests).thenAccept(result -> {
            try {
                sendResult(exchange, result);
            } catch (IOException e) {
                exchange.close();
            }
        });
    }

    private void handleLocalRequest(
            HttpExchange exchange,
            String method,
            String key
    ) throws IOException {
        byte[] value = METHOD_PUT.equals(method)
                ? exchange.getRequestBody().readAllBytes()
                : EMPTY_BODY;

        OperationResult result = executeLocal(method, key, value);
        sendResult(exchange, result);
    }

    private OperationResult executeLocal(
            String method,
            String key,
            byte[] value
    ) {
        try {
            return switch (method) {
                case METHOD_GET -> new OperationResult(HTTP_OK, dao.get(key));
                case METHOD_PUT -> {
                    dao.upsert(key, value);
                    yield new OperationResult(HTTP_CREATED, EMPTY_BODY);
                }
                case METHOD_DELETE -> {
                    dao.delete(key);
                    yield new OperationResult(HTTP_ACCEPTED, EMPTY_BODY);
                }
                default -> throw new IllegalStateException("Unexpected HTTP method: " + method);
            };
        } catch (NoSuchElementException e) {
            return new OperationResult(HTTP_NOT_FOUND, EMPTY_BODY);
        } catch (IOException e) {
            return new OperationResult(HTTP_INTERNAL_SERVER_ERROR, EMPTY_BODY);
        }
    }

    private static void sendEmptyResponse(
            HttpExchange exchange,
            int status
    ) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(status, EMPTY_RESPONSE_LENGTH);
        }
    }

    private static void sendResult(
            HttpExchange exchange,
            OperationResult result
    ) throws IOException {
        byte[] body = result.body();

        if (body.length == 0) {
            sendEmptyResponse(exchange, result.status());
            return;
        }

        try (exchange) {
            exchange.sendResponseHeaders(result.status(), body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }
    }

    private List<CompletableFuture<OperationResult>> startReplicaRequests(
            int[] ports,
            int[] replicaIndices,
            String method,
            String key,
            byte[] value
    ) {
        List<CompletableFuture<OperationResult>> requests = new ArrayList<>();
        boolean executeOnThisNode = false;

        for (int index : replicaIndices) {
            int replicaPort = ports[index];

            if (replicaPort == port) {
                executeOnThisNode = true;
            } else {
                requests.add(sendToReplica(replicaPort, method, key, value));
            }
        }

        if (executeOnThisNode) {
            OperationResult localResult = executeLocal(method, key, value);
            requests.add(CompletableFuture.completedFuture(localResult));
        }

        return requests;
    }

    private CompletableFuture<OperationResult> sendToReplica(
            int nodePort,
            String method,
            String key,
            byte[] value
    ) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create(
                "http://localhost:" + nodePort + "/v0/entity?id=" + encodedKey
        );

        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(REPLICA_TIMEOUT)
                .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE);

        switch (method) {
            case METHOD_GET -> builder.GET();
            case METHOD_PUT -> builder.PUT(HttpRequest.BodyPublishers.ofByteArray(value));
            case METHOD_DELETE -> builder.DELETE();
            default -> throw new IllegalStateException("Unexpected HTTP method: " + method);
        }

        return client.sendAsync(
                        builder.build(),
                        HttpResponse.BodyHandlers.ofByteArray()
                )
                .thenApply(response ->
                        new OperationResult(response.statusCode(), response.body())
                )
                .orTimeout(REPLICA_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(error ->
                        new OperationResult(HTTP_SERVICE_UNAVAILABLE, EMPTY_BODY)
                );
    }

    private static CompletableFuture<OperationResult> collectResults(
            String method,
            int ack,
            List<CompletableFuture<OperationResult>> requests
    ) {
        ResponseCollector collector = new ResponseCollector(method, ack, requests.size());
        for (CompletableFuture<OperationResult> request : requests) {
            request.thenAccept(collector::accept);
        }
        return collector.result;
    }

    private static final class ResponseCollector {

        private final String method;
        private final int ack;
        private final int allowedFailures;
        private final CompletableFuture<OperationResult> result = new CompletableFuture<>();
        private OperationResult selected = new OperationResult(HTTP_NOT_FOUND, EMPTY_BODY);
        private int successful;
        private int failed;

        private ResponseCollector(String method, int ack, int replicaCount) {
            this.method = method;
            this.ack = ack;
            allowedFailures = replicaCount - ack;
        }

        private synchronized void accept(OperationResult response) {
            if (result.isDone()) {
                return;
            }
            if (isSuccessful(response)) {
                if (response.status() != HTTP_NOT_FOUND) {
                    selected = response;
                }
                successful++;
                if (successful >= ack) {
                    OperationResult responseToClient = METHOD_DELETE.equals(method)
                            ? new OperationResult(HTTP_OK, EMPTY_BODY)
                            : selected;
                    result.complete(responseToClient);
                }
            } else {
                failed++;
                if (failed > allowedFailures) {
                    result.complete(new OperationResult(HTTP_SERVICE_UNAVAILABLE, EMPTY_BODY));
                }
            }
        }

        private boolean isSuccessful(OperationResult response) {
            int status = response.status();
            return switch (method) {
                case METHOD_GET -> status == HTTP_OK || status == HTTP_NOT_FOUND;
                case METHOD_PUT -> status == HTTP_CREATED;
                case METHOD_DELETE -> status == HTTP_ACCEPTED;
                default -> throw new IllegalStateException("Unexpected HTTP method: " + method);
            };
        }
    }

    private record OperationResult(int status, byte[] body) {}

}
