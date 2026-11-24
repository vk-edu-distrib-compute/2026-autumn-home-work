package company.vk.edu.distrib.compute.tomovalex.kv;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReplicationCoordinator implements AutoCloseable {
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    static final String REPLICA_HEADER = "X-Tomovalex-Replica";
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(750);
    private static final Duration QUORUM_TIMEOUT = Duration.ofSeconds(1);

    private final int port;
    private final Dao<byte[]> dao;
    private final HttpClient client;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public ReplicationCoordinator(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
        this.client = HttpClient.newBuilder().connectTimeout(REQUEST_TIMEOUT)
            .version(HttpClient.Version.HTTP_1_1).build();
    }

    public Response execute(String method, String id, byte[] body, List<Integer> replicas, int ack) {
        BlockingQueue<Response> responses = new LinkedBlockingQueue<>();
        long deadline = System.nanoTime() + QUORUM_TIMEOUT.toNanos();
        for (int replica : replicas) {
            CompletableFuture<Response> response;
            if (replica == port) {
                response = CompletableFuture.supplyAsync(() -> executeLocal(method, id, body), executor);
            } else {
                response = send(replica, method, id, body);
            }
            response.whenComplete((result, error) -> responses.add(error == null ? result : emptyResponse(503)));
        }
        return collectResponses(method, responses, replicas.size(), ack, deadline);
    }

    public Response executeLocal(String method, String id, byte[] body) {
        try {
            return switch (method) {
                case GET -> new Response(200, dao.get(id));
                case PUT -> {
                    dao.upsert(id, body);
                    yield emptyResponse(201);
                }
                case DELETE -> {
                    dao.delete(id);
                    yield emptyResponse(202);
                }
                default -> emptyResponse(405);
            };
        } catch (NoSuchElementException e) {
            return emptyResponse(404);
        } catch (IOException e) {
            return emptyResponse(500);
        }
    }

    private CompletableFuture<Response> send(int replica, String method, String id, byte[] body) {
        String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(
            URI.create("http://localhost:" + replica + "/v0/entity?id=" + encodedId))
            .timeout(REQUEST_TIMEOUT)
            .header(REPLICA_HEADER, "true")
            .method(method, PUT.equals(method)
                ? HttpRequest.BodyPublishers.ofByteArray(body) : HttpRequest.BodyPublishers.noBody())
            .build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(response -> new Response(response.statusCode(), response.body()));
    }

    private static Response collectResponses(String method, BlockingQueue<Response> responses,
                                             int count, int ack, long deadline) {
        int confirmed = 0;
        Response result = emptyResponse(GET.equals(method) ? 404 : successStatus(method));
        for (int remaining = count; remaining > 0; remaining--) {
            Response response;
            try {
                response = responses.poll(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return emptyResponse(503);
            }
            if (response == null) {
                return emptyResponse(503);
            }
            if (isConfirmed(method, response)) {
                confirmed++;
                result = selectResult(result, response);
            }
            if (confirmed >= ack) {
                return result;
            }
            if (confirmed + remaining - 1 < ack) {
                return emptyResponse(503);
            }
        }
        return emptyResponse(503);
    }

    private static boolean isConfirmed(String method, Response response) {
        return response.status() == successStatus(method) || GET.equals(method) && response.status() == 404;
    }

    private static Response selectResult(Response current, Response response) {
        return response.status() == 200 ? response : current;
    }

    private static int successStatus(String method) {
        return switch (method) {
            case GET -> 200;
            case PUT -> 201;
            case DELETE -> 202;
            default -> 405;
        };
    }

    private static Response emptyResponse(int status) {
        return new Response(status, new byte[0]);
    }

    @Override
    public void close() {
        client.close();
        executor.close();
    }

    public record Response(int status, byte[] body) {
    }
}
