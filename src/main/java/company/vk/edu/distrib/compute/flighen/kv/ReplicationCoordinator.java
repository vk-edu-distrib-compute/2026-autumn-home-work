package company.vk.edu.distrib.compute.flighen.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

final class ReplicationCoordinator {
    private static final String INTERNAL_ENTITY_PATH = "/v0/internal/entity";
    private static final String MEDIA_TYPE = "application/octet-stream";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final Duration REPLICA_TIMEOUT = Duration.ofMillis(1200);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(1500);
    private static final byte[] EMPTY_BODY = new byte[0];

    private final Dao<byte[]> dao;
    private final int mainPort;
    private final HttpClient client;

    ReplicationCoordinator(Dao<byte[]> dao, int mainPort, HttpClient client) {
        this.dao = dao;
        this.mainPort = mainPort;
        this.client = client;
    }

    OperationResult execute(
            String method,
            String key,
            int ack,
            byte[] value,
            ClusterConfiguration cluster
    ) {
        List<Integer> replicas = cluster.determineReplicas(key);
        BlockingQueue<OperationResult> completions = new LinkedBlockingQueue<>();

        for (int replicaPort : replicas) {
            if (replicaPort == mainPort) {
                completions.offer(executeLocally(method, key, value));
            } else {
                sendToReplica(replicaPort, method, key, value, completions);
            }
        }

        return awaitQuorum(method, ack, replicas.size(), cluster.replicated(), completions);
    }

    OperationResult executeLocally(String method, String key, byte[] value) {
        try {
            switch (method) {
                case GET_METHOD:
                    return new OperationResult(HttpStatusCodes.OK, dao.get(key), null);
                case PUT_METHOD:
                    dao.upsert(key, value);
                    return OperationResult.empty(HttpStatusCodes.CREATED);
                case DELETE_METHOD:
                    dao.delete(key);
                    return OperationResult.empty(HttpStatusCodes.ACCEPTED);
                default:
                    return OperationResult.empty(HttpStatusCodes.FORBIDDEN);
            }
        } catch (EmptyKeyException e) {
            return OperationResult.empty(HttpStatusCodes.BAD_REQUEST);
        } catch (NoSuchElementException e) {
            return OperationResult.empty(HttpStatusCodes.NOT_FOUND);
        } catch (IllegalArgumentException e) {
            return OperationResult.empty(HttpStatusCodes.UNPROCESSABLE_ENTITY);
        } catch (IOException | RuntimeException e) {
            return OperationResult.failure(e);
        }
    }

    private void sendToReplica(
            int replicaPort,
            String method,
            String key,
            byte[] value,
            BlockingQueue<OperationResult> completions
    ) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create(
                "http://localhost:%d%s?id=%s".formatted(replicaPort, INTERNAL_ENTITY_PATH, encodedKey)
        );
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(REPLICA_TIMEOUT);

        switch (method) {
            case GET_METHOD:
                requestBuilder.GET();
                break;
            case PUT_METHOD:
                requestBuilder
                        .header("Content-Type", MEDIA_TYPE)
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(value));
                break;
            case DELETE_METHOD:
                requestBuilder.DELETE();
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        }

        try {
            client.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray())
                    .orTimeout(REPLICA_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .whenComplete((response, error) -> {
                        if (error == null) {
                            completions.offer(
                                    new OperationResult(response.statusCode(), response.body(), null)
                            );
                        } else {
                            completions.offer(OperationResult.failure(error));
                        }
                    });
        } catch (RuntimeException e) {
            completions.offer(OperationResult.failure(e));
        }
    }

    private static OperationResult awaitQuorum(
            String method,
            int ack,
            int replicas,
            boolean replicated,
            BlockingQueue<OperationResult> completions
    ) {
        int confirmations = 0;
        int completed = 0;
        byte[] foundValue = null;
        long deadline = System.nanoTime() + REQUEST_TIMEOUT.toNanos();

        try {
            while (completed < replicas) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }

                OperationResult result = completions.poll(remaining, TimeUnit.NANOSECONDS);
                if (result == null) {
                    break;
                }

                completed++;
                if (isConfirmation(method, result)) {
                    confirmations++;
                    foundValue = updateFoundValue(method, result, foundValue);
                }

                if (confirmations >= ack) {
                    return successfulResult(method, foundValue, replicated);
                }

                int pending = replicas - completed;
                if (confirmations + pending < ack) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return OperationResult.empty(HttpStatusCodes.SERVICE_UNAVAILABLE);
    }

    private static byte[] updateFoundValue(
            String method,
            OperationResult result,
            byte[] currentValue
    ) {
        if (GET_METHOD.equals(method) && result.statusCode() == HttpStatusCodes.OK) {
            return result.body();
        }
        return currentValue;
    }

    private static boolean isConfirmation(String method, OperationResult result) {
        if (result.error() != null) {
            return false;
        }

        switch (method) {
            case GET_METHOD:
                return result.statusCode() == HttpStatusCodes.OK
                        || result.statusCode() == HttpStatusCodes.NOT_FOUND;
            case PUT_METHOD:
                return result.statusCode() == HttpStatusCodes.CREATED;
            case DELETE_METHOD:
                return result.statusCode() == HttpStatusCodes.ACCEPTED;
            default:
                return false;
        }
    }

    private static OperationResult successfulResult(String method, byte[] foundValue, boolean replicated) {
        switch (method) {
            case GET_METHOD:
                return foundValue == null
                        ? OperationResult.empty(HttpStatusCodes.NOT_FOUND)
                        : new OperationResult(HttpStatusCodes.OK, foundValue, null);
            case PUT_METHOD:
                return OperationResult.empty(HttpStatusCodes.CREATED);
            case DELETE_METHOD:
                return OperationResult.empty(
                        replicated ? HttpStatusCodes.OK : HttpStatusCodes.ACCEPTED
                );
            default:
                return OperationResult.empty(HttpStatusCodes.INTERNAL_SERVER_ERROR);
        }
    }

    record OperationResult(int statusCode, byte[] body, Throwable error) {
        static OperationResult empty(int statusCode) {
            return new OperationResult(statusCode, EMPTY_BODY, null);
        }

        static OperationResult failure(Throwable error) {
            return new OperationResult(0, EMPTY_BODY, error);
        }
    }
}
