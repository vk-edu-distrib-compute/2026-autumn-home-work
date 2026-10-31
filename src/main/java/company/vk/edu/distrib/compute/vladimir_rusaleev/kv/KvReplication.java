package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;

final class KvReplication implements AutoCloseable {
    private static final String REPLICA_HEADER = "X-Replica";
    private static final String REPLICA_VALUE = "1";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    private static final long WAIT_NANOS = Duration.ofSeconds(2).toNanos();

    private final int port;
    private final ExecutorService coordinators = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(TIMEOUT)
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    KvReplication(int port) {
        this.port = port;
    }

    boolean handle(HttpExchange exchange) throws IOException {
        KvRequest request = KvRequest.parse(exchange.getRequestURI().getRawQuery(), port);
        String method = exchange.getRequestMethod();
        boolean internal = REPLICA_VALUE.equals(exchange.getRequestHeaders().getFirst(REPLICA_HEADER));

        if (internal || !request.configured() || !supported(method)) {
            return false;
        }

        byte[] body = exchange.getRequestBody().readAllBytes();
        coordinators.execute(() -> respond(exchange, request, method, body));
        return true;
    }

    private void respond(HttpExchange exchange, KvRequest request, String method, byte[] body) {
        try (exchange) {
            Reply reply = coordinate(request.replicas(), request.key(), request.ack(), method, body);
            KvServiceImpl.respond(exchange, reply.status(), reply.body());
        } catch (IOException exception) {
            exchange.close();
        }
    }

    private Reply coordinate(int[] replicas, String key, int ack, String method, byte[] body) {
        BlockingQueue<Reply> replies = new LinkedBlockingQueue<>();

        for (int replica : replicas) {
            send(replica, key, method, body, replies);
        }

        return collect(replies, replicas.length, ack, method);
    }

    private void send(int replica, String key, String method, byte[] body, BlockingQueue<Reply> replies) {
        URI uri = URI.create("http://localhost:" + replica + "/v0/entity?id="
            + URLEncoder.encode(key, StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(TIMEOUT)
            .header(REPLICA_HEADER, REPLICA_VALUE)
            .method(method, HttpRequest.BodyPublishers.ofByteArray(body))
            .build();

        client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .whenComplete((response, exception) -> replies.add(exception == null
                ? new Reply(response.statusCode(), response.body())
                : new Reply(Reply.UNAVAILABLE)));
    }

    private static Reply collect(BlockingQueue<Reply> replies, int count, int ack, String method) {
        int successes = 0;
        int remaining = count;
        Reply found = new Reply(Reply.NOT_FOUND);
        long deadline = System.nanoTime() + WAIT_NANOS;

        try {
            while (remaining > 0) {
                Reply reply = poll(replies, deadline);
                if (reply == null) {
                    break;
                }

                remaining--;

                if (successful(reply, method)) {
                    successes++;
                    found = readValue(found, reply, method);
                }

                if (successes >= ack) {
                    return GET_METHOD.equals(method)
                        ? found
                        : new Reply(coordinatorStatus(method));
                }

                if (successes + remaining < ack) {
                    break;
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }

        return new Reply(Reply.UNAVAILABLE);
    }

    private static Reply poll(BlockingQueue<Reply> replies, long deadline) throws InterruptedException {
        long wait = deadline - System.nanoTime();
        return wait <= 0 ? null : replies.poll(wait, TimeUnit.NANOSECONDS);
    }

    private static Reply readValue(Reply found, Reply reply, String method) {
        return GET_METHOD.equals(method) && reply.status() == Reply.OK ? reply : found;
    }

    private static boolean successful(Reply reply, String method) {
        return switch (method) {
            case "GET" -> reply.status() == Reply.OK || reply.status() == Reply.NOT_FOUND;
            case "PUT" -> reply.status() == Reply.CREATED;
            case "DELETE" -> reply.status() == Reply.ACCEPTED;
            default -> false;
        };
    }

    private static int coordinatorStatus(String method) {
        return PUT_METHOD.equals(method) ? Reply.CREATED : Reply.OK;
    }

    private static boolean supported(String method) {
        return switch (method) {
            case "GET", "PUT", "DELETE" -> true;
            default -> false;
        };
    }

    @Override
    public void close() {
        coordinators.close();
        client.close();
    }

    record Reply(int status, byte[] body) {
        static final int OK = 200;
        static final int CREATED = 201;
        static final int ACCEPTED = 202;
        static final int NOT_FOUND = 404;
        static final int UNAVAILABLE = 503;

        Reply(int status) {
            this(status, new byte[0]);
        }
    }
}
