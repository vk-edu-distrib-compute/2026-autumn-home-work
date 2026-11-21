package company.vk.edu.distrib.compute.sh4rrkyyyy.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.HttpConstants;
import company.vk.edu.distrib.compute.sh4rrkyyyy.common.HttpStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

final class Sh4rkyReplicator implements AutoCloseable {
    private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(200);
    private static final Duration REPLICA_TIMEOUT = Duration.ofSeconds(1);
    private static final String LOCAL_URL_PREFIX = "http://localhost:";
    private static final String ID_QUERY_PREFIX = "?" + HttpConstants.ID_PARAM + "=";
    private static final String UNSUPPORTED_METHOD_MESSAGE = "Unsupported method: ";

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();
    private final int port;
    private final Dao<byte[]> dao;
    private final ExecutorService executor;

    Sh4rkyReplicator(int port, Dao<byte[]> dao, ExecutorService executor) {
        this.port = port;
        this.dao = dao;
        this.executor = executor;
    }

    ReplicaResponse replicate(String method, String id, byte[] data, int[] replicas, int ack) {
        var responses = new ExecutorCompletionService<ReplicaResponse>(executor);
        for (int replica : replicas) {
            responses.submit(() -> requestReplica(replica, method, id, data));
        }
        int expectedStatus = getExpectedStatus(method);
        int confirmations = 0;
        ReplicaResponse result = new ReplicaResponse(
                HttpConstants.GET.equals(method) ? HttpStatus.NOT_FOUND : expectedStatus, HttpConstants.EMPTY_BODY);
        for (int i = 0; i < replicas.length && confirmations < ack; ++i) {
            ReplicaResponse response = nextResponse(responses);
            if (isConfirmation(method, expectedStatus, response.statusCode())) {
                ++confirmations;
                if (response.statusCode() == expectedStatus) {
                    result = response;
                }
            }
        }
        if (confirmations < ack) {
            return new ReplicaResponse(HttpStatus.SERVICE_UNAVAILABLE, HttpConstants.EMPTY_BODY);
        }
        return successfulResponse(method, replicas.length, result);
    }

    private static int getExpectedStatus(String method) {
        return switch (method) {
            case HttpConstants.PUT -> HttpStatus.CREATED;
            case HttpConstants.DELETE -> HttpStatus.ACCEPTED;
            default -> HttpStatus.OK;
        };
    }

    private static boolean isConfirmation(String method, int expectedStatus, int status) {
        return status == expectedStatus || HttpConstants.GET.equals(method) && status == HttpStatus.NOT_FOUND;
    }

    private static ReplicaResponse successfulResponse(String method, int replicaCount, ReplicaResponse response) {
        if (HttpConstants.DELETE.equals(method) && replicaCount > 1) {
            return new ReplicaResponse(HttpStatus.OK, HttpConstants.EMPTY_BODY);
        }
        return response;
    }

    ReplicaResponse handleLocal(String method, String key, byte[] data) throws IOException {
        switch (method) {
            case HttpConstants.GET -> {
                try {
                    return new ReplicaResponse(HttpStatus.OK, dao.get(key));
                } catch (NoSuchElementException e) {
                    return new ReplicaResponse(HttpStatus.NOT_FOUND, HttpConstants.EMPTY_BODY);
                }
            }
            case HttpConstants.PUT -> {
                dao.upsert(key, data);
                return new ReplicaResponse(HttpStatus.CREATED, HttpConstants.EMPTY_BODY);
            }
            case HttpConstants.DELETE -> {
                dao.delete(key);
                return new ReplicaResponse(HttpStatus.ACCEPTED, HttpConstants.EMPTY_BODY);
            }
            default -> throw new IllegalArgumentException(UNSUPPORTED_METHOD_MESSAGE + method);
        }
    }

    @Override
    public void close() {
        client.shutdownNow();
    }

    private ReplicaResponse requestReplica(int replica, String method, String id, byte[] data)
            throws IOException, InterruptedException {
        if (replica == port) {
            return handleLocal(method, id, data);
        }
        String encoded = URLEncoder.encode(id, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(LOCAL_URL_PREFIX + replica + HttpConstants.ENTITY_PATH + ID_QUERY_PREFIX + encoded))
                .header(HttpConstants.REPLICA_HEADER, HttpConstants.REPLICA_HEADER_VALUE)
                .timeout(REPLICA_TIMEOUT)
                .method(method, HttpRequest.BodyPublishers.ofByteArray(data))
                .build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        return new ReplicaResponse(response.statusCode(), response.body());
    }

    private static ReplicaResponse nextResponse(ExecutorCompletionService<ReplicaResponse> responses) {
        try {
            return responses.take().get();
        } catch (ExecutionException e) {
            return new ReplicaResponse(HttpStatus.SERVICE_UNAVAILABLE, HttpConstants.EMPTY_BODY);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new ReplicaResponse(HttpStatus.SERVICE_UNAVAILABLE, HttpConstants.EMPTY_BODY);
        }
    }

    record ReplicaResponse(int statusCode, byte[] body) {
    }
}
