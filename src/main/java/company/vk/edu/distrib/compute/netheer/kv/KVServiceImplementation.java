package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public final class KVServiceImplementation implements KVService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    private static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private final ExecutorService executor;

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final int port;

    private static final Duration REPLICA_TIMEOUT = Duration.ofSeconds(1);
    private final HttpClient replicaClient = HttpClient.newBuilder()
            .connectTimeout(REPLICA_TIMEOUT)
            .build();
    private final ExecutorService replicaExecutor =
            Executors.newVirtualThreadPerTaskExecutor();

    public KVServiceImplementation(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.port = port;

        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);

        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        replicaClient.shutdownNow();

        executor.close();
        replicaExecutor.close();

        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ReplicaSelector readReplicaSelector() {
        String cluster = System.getProperty("CLUSTER");
        if (cluster == null) {
            return new ReplicaSelector(new int[]{port}, 1);
        }

        String[] parts = cluster.split(",", -1);
        int[] ports = new int[parts.length];
        for (int i = 0; i < parts.length; ++i) {
            ports[i] = Integer.parseInt(parts[i].trim());
        }

        String factor = System.getProperty("REPLICATION_FACTOR");
        if (factor == null) {
            return new ReplicaSelector(ports, ports.length);
        }
        return new ReplicaSelector(ports, Integer.parseInt(factor.trim()));
    }

    private ReplicaResult executeLocal(
            String method,
            String key,
            byte[] body
    ) throws IOException {
        switch (method) {
            case GET_METHOD -> {
                try {
                    byte[] value = dao.get(key);
                    return new ReplicaResult(HTTP_OK, value);
                } catch (NoSuchElementException e) {
                    return new ReplicaResult(HTTP_NOT_FOUND, new byte[0]);
                }
            }
            case PUT_METHOD -> {
                dao.upsert(key, body);
                return new ReplicaResult(HTTP_CREATED, new byte[0]);
            }
            case DELETE_METHOD -> {
                dao.delete(key);
                return new ReplicaResult(HTTP_ACCEPTED, new byte[0]);
            }
            default -> {
                return new ReplicaResult(HTTP_METHOD_NOT_ALLOWED, new byte[0]);
            }
        }
    }

    private static void sendReplicaResult(
            HttpExchange exchange,
            ReplicaResult result
    ) throws IOException {
        if (result.body().length == 0) {
            sendEmptyResponse(exchange, result.statusCode());
            return;
        }
        sendResponse(exchange, result.statusCode(), result.body());
    }

    private CompletableFuture<ReplicaResult> sendRemote(
            int targetPort,
            String method,
            String key,
            byte[] body
    ) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create(
                "http://localhost:" + targetPort + ENTITY_PATH + "?id=" + encodedKey
        );

        HttpRequest.BodyPublisher publisher = PUT_METHOD.equals(method)
                ? HttpRequest.BodyPublishers.ofByteArray(body)
                : HttpRequest.BodyPublishers.noBody();

        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(REPLICA_TIMEOUT)
                .header(INTERNAL_REQUEST_HEADER, "true")
                .method(method, publisher)
                .build();

        return replicaClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response ->
                        new ReplicaResult(response.statusCode(), response.body())
                );
    }

    private List<CompletableFuture<ReplicaResult>> sendToReplicas(
            ReplicaSelector selector,
            String method,
            String key,
            byte[] body
    ) {
        List<Integer> replicaPorts = selector.selectReplicas(key);
        List<CompletableFuture<ReplicaResult>> requests =
                new ArrayList<>(replicaPorts.size());

        for (int replicaPort : replicaPorts) {
            if (replicaPort == port) {
                CompletableFuture<ReplicaResult> localRequest =
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                return executeLocal(method, key, body);
                            } catch (IOException e) {
                                throw new CompletionException(e);
                            }
                        }, replicaExecutor);

                requests.add(localRequest);
            } else {
                requests.add(sendRemote(replicaPort, method, key, body));
            }
        }

        return requests;
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
                sendEmptyResponse(exchange, HTTP_NOT_FOUND);
                return;
            }

            if (!GET_METHOD.equals(exchange.getRequestMethod())) {
                sendEmptyResponse(exchange, HTTP_METHOD_NOT_ALLOWED);
                return;
            }

            sendEmptyResponse(exchange, HTTP_OK);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                processEntityRequest(exchange);
            } catch (NoSuchElementException e) {
                sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            } catch (IllegalArgumentException e) {
                sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
            } catch (IOException e) {
                sendEmptyResponse(exchange, HTTP_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void processEntityRequest(HttpExchange exchange) throws IOException {
        if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            return;
        }

        String internalHeader =
                exchange.getRequestHeaders().getFirst(INTERNAL_REQUEST_HEADER);
        if ("true".equals(internalHeader)) {
            processInternalRequest(exchange);
            return;
        }

        ReplicaSelector replicaSelector;
        try {
            replicaSelector = readReplicaSelector();
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, HTTP_INTERNAL_SERVER_ERROR);
            return;
        }
        EntityParams params = EntityParams.parse(
                exchange.getRequestURI().getRawQuery(),
                replicaSelector.replicationFactor()
        );
        String key = params.key();

        String method = exchange.getRequestMethod();
        byte[] body = new byte[0];
        if (PUT_METHOD.equals(method)) {
            body = exchange.getRequestBody().readAllBytes();
        }

        if (!isSupportedMethod(method)) {
            sendEmptyResponse(exchange, HTTP_METHOD_NOT_ALLOWED);
            return;
        }

        List<CompletableFuture<ReplicaResult>> requests =
                sendToReplicas(replicaSelector, method, key, body);

        ReplicaResult result;
        try {
            ReplicaResponseCollector collector =
                    new ReplicaResponseCollector(method, params.ack());

            result = collector.collect(requests, REPLICA_TIMEOUT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendEmptyResponse(exchange, HTTP_SERVICE_UNAVAILABLE);
            return;
        }

        sendReplicaResult(exchange, result);
    }

    private void processInternalRequest(HttpExchange exchange) throws IOException {
        EntityParams params = EntityParams.parse(
                exchange.getRequestURI().getRawQuery(),
                1
        );

        String method = exchange.getRequestMethod();
        byte[] body = new byte[0];
        if (PUT_METHOD.equals(method)) {
            body = exchange.getRequestBody().readAllBytes();
        }

        ReplicaResult result = executeLocal(method, params.key(), body);
        sendReplicaResult(exchange, result);
    }

    private static boolean isSupportedMethod(String method) {
        return GET_METHOD.equals(method)
                || PUT_METHOD.equals(method)
                || DELETE_METHOD.equals(method);
    }

    private static void sendEmptyResponse(HttpExchange exchange, int statusCode)
            throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
    }

    private static void sendResponse(
            HttpExchange exchange,
            int statusCode,
            byte[] body
    ) throws IOException {
        exchange.sendResponseHeaders(statusCode, body.length);

        try (OutputStream responseBody = exchange.getResponseBody()) {
            responseBody.write(body);
        }
    }
}
