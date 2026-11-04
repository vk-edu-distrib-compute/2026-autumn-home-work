package company.vk.edu.distrib.compute.nosorozhek.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class ReplicationCoordinator implements AutoCloseable {
    public static final String INTERNAL_HEADER = "X-Internal-Replication";

    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);
    private static final int HTTP_OK = 200;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;

    private final Dao<byte[]> localDao;
    private final int port;
    private final HttpClient client;

    public ReplicationCoordinator(Dao<byte[]> localDao, int port) {
        this.localDao = localDao;
        this.port = port;
        this.client = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
    }

    public static boolean isInternalRequest(com.sun.net.httpserver.HttpExchange exchange) {
        return exchange.getRequestHeaders().getFirst(INTERNAL_HEADER) != null;
    }

    public Optional<byte[]> get(String key, int ack, boolean internal) throws IOException {
        if (internal) {
            try {
                return Optional.of(localDao.get(key));
            } catch (NoSuchElementException e) {
                return Optional.empty();
            }
        }

        List<Integer> ports = clusterPorts();
        validateAck(ack, ports.size());
        List<CompletableFuture<ReplicaResult>> requests = new ArrayList<>(ports.stream()
                .map(replicaPort -> {
                    if (replicaPort == port) {
                        return CompletableFuture.completedFuture(localRead(key));
                    } else {
                        return sendRead(replicaPort, key);
                    }
                })
                .toList());

        return awaitQuorum(requests, ack).stream()
                .map(ReplicaResult::value)
                .filter(Objects::nonNull)
                .findFirst();
    }

    public void upsert(String key, byte[] value, int ack, boolean internal) throws IOException {
        write(key, value, ack, internal, HTTP_CREATED, () -> localDao.upsert(key, value));
    }

    public void delete(String key, int ack, boolean internal) throws IOException {
        write(key, null, ack, internal, HTTP_ACCEPTED, () -> localDao.delete(key));
    }

    private void write(
            String key,
            byte[] value,
            int ack,
            boolean internal,
            int expectedStatus,
            IoOperation operation
    ) throws IOException {
        if (internal) {
            operation.run();
            return;
        }

        List<Integer> ports = clusterPorts();
        validateAck(ack, ports.size());
        List<CompletableFuture<ReplicaResult>> requests = new ArrayList<>(ports.stream()
                .map(replicaPort -> {
                    if (replicaPort == port) {
                        return CompletableFuture.completedFuture(localWrite(operation));
                    } else {
                        return sendWrite(replicaPort, key, value, expectedStatus);
                    }
                }).toList());
        awaitQuorum(requests, ack);
    }

    public boolean isReplicated() {
        return clusterPorts().size() > 1;
    }

    private List<ReplicaResult> awaitQuorum(
            List<CompletableFuture<ReplicaResult>> requests,
            int required
    ) throws IOException {
        List<CompletableFuture<ReplicaResult>> pending = new ArrayList<>(requests);
        List<ReplicaResult> acknowledged = new ArrayList<>();
        while (!pending.isEmpty() && acknowledged.size() < required) {
            CompletableFuture.anyOf(pending.toArray(CompletableFuture<?>[]::new)).join();
            for (var iterator = pending.iterator(); iterator.hasNext(); ) {
                CompletableFuture<ReplicaResult> request = iterator.next();
                if (!request.isDone()) {
                    continue;
                }
                ReplicaResult result = request.join();
                iterator.remove();
                if (result.acknowledged()) {
                    acknowledged.add(result);
                }
            }
        }
        if (acknowledged.size() < required) {
            throw new QuorumUnavailableException();
        }
        return acknowledged;
    }

    private ReplicaResult localRead(String key) {
        try {
            return new ReplicaResult(true, localDao.get(key));
        } catch (NoSuchElementException e) {
            return new ReplicaResult(true, null);
        } catch (IOException | IllegalArgumentException e) {
            return new ReplicaResult(false, null);
        }
    }

    private static ReplicaResult localWrite(IoOperation operation) {
        try {
            operation.run();
            return new ReplicaResult(true, null);
        } catch (IOException | IllegalArgumentException e) {
            return new ReplicaResult(false, null);
        }
    }

    private CompletableFuture<ReplicaResult> sendRead(int replicaPort, String key) {
        HttpRequest request = request(replicaPort, key)
                .GET()
                .header(INTERNAL_HEADER, "1")
                .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .orTimeout(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .handle((response, error) -> {
                    if (error != null) {
                        return new ReplicaResult(false, null);
                    }
                    if (response.statusCode() == HTTP_OK) {
                        return new ReplicaResult(true, response.body());
                    }
                    return new ReplicaResult(response.statusCode() == HTTP_NOT_FOUND, null);
                });
    }

    private CompletableFuture<ReplicaResult> sendWrite(int replicaPort, String key, byte[] value, int expectedStatus) {
        HttpRequest.Builder request = request(replicaPort, key)
                .header(INTERNAL_HEADER, "1");
        if (value == null) {
            request.DELETE();
        } else {
            request.PUT(HttpRequest.BodyPublishers.ofByteArray(value));
        }
        return client.sendAsync(request.build(), HttpResponse.BodyHandlers.discarding())
                .orTimeout(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .handle((response, error) -> new ReplicaResult(
                        error == null && response.statusCode() == expectedStatus,
                        null));
    }

    private HttpRequest.Builder request(int replicaPort, String key) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create("http://localhost:" + replicaPort + "/v0/entity?id=" + encodedKey);
        return HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT);
    }

    private List<Integer> clusterPorts() {
        String clusters = System.getProperty(CLUSTER_PROPERTY);
        if (clusters == null || clusters.isBlank()) {
            return List.of(port);
        }

        return Arrays.stream(clusters.split(",", -1))
                .map(String::strip)
                .mapToInt(Integer::parseInt)
                .boxed()
                .sorted()
                .toList();
    }

    private static void validateAck(int ack, int clusterSize) {
        if (ack < 1 || ack > clusterSize) {
            throw new IllegalArgumentException("ack must be between 1 and the cluster size");
        }
    }

    @Override
    public void close() {
        client.close();
    }

    @FunctionalInterface
    private interface IoOperation {
        void run() throws IOException;
    }

    private record ReplicaResult(boolean acknowledged, byte[] value) {
    }
}
