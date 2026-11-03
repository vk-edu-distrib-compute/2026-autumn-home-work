package company.vk.edu.distrib.compute.cloudyy74.kv;

import company.vk.edu.distrib.compute.Dao;

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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

final class Cloudyy74Replication implements AutoCloseable {
    static final String REPLICA_HEADER = "X-Cloudyy74-Replica";
    static final String REPLICA_HEADER_VALUE = "true";
    static final String GET_METHOD = "GET";
    static final String PUT_METHOD = "PUT";
    static final String DELETE_METHOD = "DELETE";

    static final int OK_STATUS = 200;
    static final int CREATED_STATUS = 201;
    static final int ACCEPTED_STATUS = 202;
    static final int NOT_FOUND_STATUS = 404;
    static final int UNAVAILABLE_STATUS = 503;
    private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(300);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(700);
    private static final Duration QUORUM_TIMEOUT = Duration.ofSeconds(1);

    private final int port;
    private final Dao<byte[]> dao;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build();

    Cloudyy74Replication(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    Response local(String method, String key, byte[] body) throws IOException {
        return switch (method) {
            case GET_METHOD -> {
                try {
                    yield new Response(OK_STATUS, dao.get(key));
                } catch (NoSuchElementException e) {
                    yield Response.empty(NOT_FOUND_STATUS);
                }
            }
            case PUT_METHOD -> {
                dao.upsert(key, body);
                yield Response.empty(CREATED_STATUS);
            }
            case DELETE_METHOD -> {
                dao.delete(key);
                yield Response.empty(ACCEPTED_STATUS);
            }
            default -> throw new IllegalArgumentException("Unsupported method");
        };
    }

    Response coordinate(String method, String key, byte[] body, Cloudyy74ClusterConfig config, int ack)
            throws IOException {
        final var replies = new ExecutorCompletionService<Response>(executor);
        final var replicas = config.replicas(key);
        for (int replica : replicas) {
            replies.submit(() -> replica == port ? local(method, key, body) : remote(replica, method, key, body));
        }
        try {
            return collect(replies, method, replicas.size(), ack);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for replicas", e);
        }
    }

    private Response collect(ExecutorCompletionService<Response> replies, String method, int count, int ack)
            throws InterruptedException {
        final var deadline = System.nanoTime() + QUORUM_TIMEOUT.toNanos();
        int confirmed = 0;
        int remaining = count;
        Response result = Response.empty(NOT_FOUND_STATUS);
        while (remaining > 0) {
            final var reply = replies.poll(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (reply == null) {
                return Response.empty(UNAVAILABLE_STATUS);
            }
            remaining--;
            Response response;
            try {
                response = reply.get();
            } catch (ExecutionException e) {
                response = Response.empty(UNAVAILABLE_STATUS);
            }
            if (isConfirmed(method, response.status())) {
                confirmed++;
                if (response.status() != NOT_FOUND_STATUS) {
                    result = response;
                }
            }
            if (confirmed >= ack) {
                return result;
            }
            if (confirmed + remaining < ack) {
                return Response.empty(UNAVAILABLE_STATUS);
            }
        }
        return Response.empty(UNAVAILABLE_STATUS);
    }

    private static boolean isConfirmed(String method, int status) {
        return switch (method) {
            case GET_METHOD -> status == OK_STATUS || status == NOT_FOUND_STATUS;
            case PUT_METHOD -> status == CREATED_STATUS;
            case DELETE_METHOD -> status == ACCEPTED_STATUS;
            default -> false;
        };
    }

    private Response remote(int replica, String method, String key, byte[] body)
            throws IOException, InterruptedException {
        final var encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        final var uri = URI.create("http://localhost:%d/v0/entity?id=%s".formatted(replica, encodedKey));
        final var publisher = PUT_METHOD.equals(method)
                ? HttpRequest.BodyPublishers.ofByteArray(body) : HttpRequest.BodyPublishers.noBody();
        final var request = HttpRequest.newBuilder(uri)
                .timeout(REQUEST_TIMEOUT)
                .header(REPLICA_HEADER, REPLICA_HEADER_VALUE)
                .method(method, publisher)
                .build();
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        return new Response(response.statusCode(), response.body());
    }

    @Override
    public void close() {
        executor.close();
        httpClient.close();
    }

    record Response(int status, byte[] body) {
        static Response empty(int status) {
            return new Response(status, new byte[0]);
        }
    }
}
