package company.vk.edu.distrib.compute.dzolin.kv;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

final class ReplicationManager implements AutoCloseable {
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int NOT_FOUND = 404;
    private static final int SERVICE_UNAVAILABLE = 503;
    private static final int DEFAULT_ACK = 1;
    private static final int CONNECT_TIMEOUT_MS = 500;
    private static final int REQUEST_TIMEOUT_MS = 1200;
    private static final int QUORUM_TIMEOUT_MS = 1500;
    private static final ReplicaResponse FAILED_RESPONSE = new ReplicaResponse(0, new byte[0]);
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(CONNECT_TIMEOUT_MS)).build();
    private final int port;
    private final PersistentDao dao;

    ReplicationManager(int port, PersistentDao dao) {
        this.port = port;
        this.dao = dao;
    }

    List<Integer> getCluster() {
        var configured = System.getProperty("CLUSTER");
        if (configured == null || configured.isEmpty()) {
            return List.of(port);
        }
        List<Integer> cluster = new ArrayList<>();
        for (var node : configured.split(",")) {
            cluster.add(Integer.parseInt(node));
        }
        return cluster;
    }

    int getAck(HttpExchange exchange, int replicas) {
        var parameter = KVServiceImpl.getQueryParameter(exchange, "ack");
        int ack;
        try {
            ack = parameter.isEmpty() && !hasAck(exchange) ? DEFAULT_ACK : Integer.parseInt(parameter);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("invalid ack", exception);
        }
        if (ack < 1 || ack > replicas) {
            throw new IllegalArgumentException("invalid ack");
        }
        return ack;
    }

    private static boolean hasAck(HttpExchange exchange) {
        var query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return false;
        }
        for (var parameter : query.split("&")) {
            var parts = parameter.split("=", 2);
            if ("ack".equals(URLDecoder.decode(parts[0], StandardCharsets.UTF_8))) {
                return true;
            }
        }
        return false;
    }

    void handle(HttpExchange exchange, String method, String id, byte[] body,
                List<Integer> cluster, int ack) throws IOException {
        BlockingQueue<ReplicaResponse> responses = new LinkedBlockingQueue<>();
        sendRequests(method, id, body, cluster, responses);
        var outcome = collect(method, ack, cluster.size(), responses);
        if (outcome.received() < ack) {
            KVServiceImpl.sendResponse(exchange, SERVICE_UNAVAILABLE, new byte[0]);
        } else if (GET.equals(method)) {
            var value = outcome.value();
            KVServiceImpl.sendResponse(exchange, value == null ? NOT_FOUND : OK,
                    value == null ? new byte[0] : value);
        } else {
            KVServiceImpl.sendResponse(exchange, PUT.equals(method) ? CREATED : OK, new byte[0]);
        }
    }

    private void sendRequests(String method, String id, byte[] body, List<Integer> cluster,
                              BlockingQueue<ReplicaResponse> responses) {
        var path = "/v0/entity?id=" + URLEncoder.encode(id, StandardCharsets.UTF_8) + "&local=1";
        for (var node : cluster) {
            if (node == port) {
                responses.add(handleLocal(method, id, body));
                continue;
            }
            var request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + node + path))
                    .timeout(Duration.ofMillis(REQUEST_TIMEOUT_MS))
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
            client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                    .whenComplete((response, error) -> {
                        if (error != null) {
                            responses.add(FAILED_RESPONSE);
                        } else {
                            responses.add(new ReplicaResponse(response.statusCode(), response.body()));
                        }
                    });
        }
    }

    private ReplicaResponse handleLocal(String method, String id, byte[] body) {
        try {
            if (GET.equals(method)) {
                return new ReplicaResponse(OK, dao.get(id));
            }
            if (PUT.equals(method)) {
                dao.upsert(id, body);
                return new ReplicaResponse(CREATED, new byte[0]);
            }
            dao.delete(id);
            return new ReplicaResponse(ACCEPTED, new byte[0]);
        } catch (NoSuchElementException exception) {
            return new ReplicaResponse(NOT_FOUND, new byte[0]);
        } catch (IOException exception) {
            return FAILED_RESPONSE;
        }
    }

    private static Outcome collect(String method, int ack, int total,
                                   BlockingQueue<ReplicaResponse> responses) throws IOException {
        int received = 0;
        int completed = 0;
        byte[] value = null;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(QUORUM_TIMEOUT_MS);
        while (received < ack && completed < total) {
            ReplicaResponse result;
            try {
                result = responses.poll(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("replication interrupted", exception);
            }
            if (result == null) {
                break;
            }
            completed++;
            if (isAcknowledged(method, result)) {
                received++;
                if (result.status() == OK) {
                    value = result.body();
                }
            }
        }
        return new Outcome(received, value);
    }

    private static boolean isAcknowledged(String method, ReplicaResponse response) {
        if (GET.equals(method)) {
            return response.status() == OK || response.status() == NOT_FOUND;
        }
        return response.status() == (PUT.equals(method) ? CREATED : ACCEPTED);
    }

    @Override
    public void close() {
        client.close();
    }

    private record ReplicaResponse(int status, byte[] body) {
    }

    private record Outcome(int received, byte[] value) {
    }
}
