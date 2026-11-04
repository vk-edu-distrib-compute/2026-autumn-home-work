package company.vk.edu.distrib.compute.akhunzianov.kv;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.net.httpserver.HttpExchange;

public class Replication implements AutoCloseable {

    private static final String CLUSTER = "CLUSTER";
    private static final String REPLICAS = "KV_REPLICAS";
    private static final String INTERNAL = "X-Replica-Request";
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final int OK_CODE = 200;
    private static final int CREATED_CODE = 201;
    private static final int ACCEPTED_CODE = 202;
    private static final int BAD_REQUEST_CODE = 400;
    private static final int NOT_FOUND_CODE = 404;
    private static final int METHOD_NOT_ALLOWED_CODE = 405;
    private static final int TOO_LARGE_CODE = 413;
    private static final int UNAVAILABLE_CODE = 503;
    private static final int MAX_BODY = 1 << 20;
    private static final int DEFAULT_ACK = 1;
    private static final int PARAM_FIELDS = 2;
    private static final int NO_BODY = -1;
    private static final Duration TIMEOUT = Duration.ofMillis(500);
    private static final byte[] EMPTY_BODY = new byte[0];

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    private final int port;

    public Replication(int port) {
        this.port = port;
    }

    public boolean handle(HttpExchange exchange, String key) throws IOException {
        if (exchange.getRequestHeaders().containsKey(INTERNAL)) {
            return false;
        }
        var cluster = System.getProperty(CLUSTER, System.getenv().getOrDefault(CLUSTER, ""));
        if (cluster.isBlank()) {
            return handleSingle(exchange);
        }
        coordinate(exchange, key, cluster);
        return true;
    }

    private static boolean handleSingle(HttpExchange exchange) throws IOException {
        if (ackOf(exchange.getRequestURI()) != DEFAULT_ACK) {
            finish(exchange, BAD_REQUEST_CODE, EMPTY_BODY);
            return true;
        }
        return false;
    }

    private void coordinate(HttpExchange exchange, String key, String cluster) throws IOException {
        var ports = Arrays.stream(cluster.split(",")).map(String::strip).mapToInt(Integer::parseInt).toArray();
        var count = replicaCount(ports.length);
        var ack = ackOf(exchange.getRequestURI());
        if (ack < DEFAULT_ACK || ack > count) {
            finish(exchange, BAD_REQUEST_CODE, EMPTY_BODY);
            return;
        }
        var method = exchange.getRequestMethod();
        if (!supported(method)) {
            finish(exchange, METHOD_NOT_ALLOWED_CODE, EMPTY_BODY);
            return;
        }
        var body = PUT.equals(method) ? exchange.getRequestBody().readNBytes(MAX_BODY + 1) : EMPTY_BODY;
        if (body.length > MAX_BODY) {
            finish(exchange, TOO_LARGE_CODE, EMPTY_BODY);
            return;
        }
        forward(exchange, key, ports, count, ack, body);
    }

    private static boolean supported(String method) {
        return GET.equals(method) || PUT.equals(method) || DELETE.equals(method);
    }

    private void forward(HttpExchange exchange, String key, int[] ports, int count, int ack, byte[] body) {
        var result = new Replies(exchange, count, ack);
        var first = ClusterDao.ownerIndex(key, ports.length);
        for (int i = 0; i < count; i++) {
            var target = ports[(first + i) % ports.length];
            var uri = URI.create("http://localhost:" + target + "/v0/entity?id="
                + URLEncoder.encode(key, StandardCharsets.UTF_8));
            var request = HttpRequest.newBuilder(uri).timeout(TIMEOUT).header(INTERNAL, Integer.toString(port))
                .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(body)).build();
            client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).whenComplete((response, error) -> {
                if (error == null) {
                    result.accept(response.statusCode(), response.body());
                } else {
                    result.accept(UNAVAILABLE_CODE, EMPTY_BODY);
                }
            });
        }
    }

    private static int replicaCount(int size) {
        var value = System.getProperty(REPLICAS, System.getenv().getOrDefault(REPLICAS, Integer.toString(size)));
        var count = Integer.parseInt(value);
        if (count < DEFAULT_ACK || count > size) {
            throw new IllegalArgumentException("Invalid replica count");
        }
        return count;
    }

    private static int ackOf(URI uri) {
        var query = uri.getRawQuery();
        if (query == null) {
            return DEFAULT_ACK;
        }
        for (var part : query.split("&")) {
            var pair = part.split("=", 2);
            if ("ack".equals(URLDecoder.decode(pair[0], StandardCharsets.UTF_8))) {
                try {
                    return pair.length == PARAM_FIELDS
                        ? Integer.parseInt(URLDecoder.decode(pair[1], StandardCharsets.UTF_8)) : 0;
                } catch (IllegalArgumentException e) {
                    return 0;
                }
            }
        }
        return DEFAULT_ACK;
    }

    private static void finish(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(code, body.length == 0 ? NO_BODY : body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private static final class Replies {

        private final ReentrantLock lock = new ReentrantLock();
        private final HttpExchange exchange;
        private final boolean read;
        private final int total;
        private final int ack;
        private int received;
        private int good;
        private boolean found;
        private boolean done;
        private byte[] value = EMPTY_BODY;

        private Replies(HttpExchange exchange, int total, int ack) {
            this.exchange = exchange;
            this.read = GET.equals(exchange.getRequestMethod());
            this.total = total;
            this.ack = ack;
        }

        private void accept(int code, byte[] body) {
            lock.lock();
            try {
                if (done) {
                    return;
                }
                received++;
                record(code, body);
                if (good >= ack) {
                    respond(successCode());
                } else if (good + total - received < ack) {
                    respond(UNAVAILABLE_CODE);
                }
            } finally {
                lock.unlock();
            }
        }

        private void record(int code, byte[] body) {
            if (!successful(code)) {
                return;
            }
            good++;
            if (read && code == OK_CODE) {
                found = true;
                value = body;
            }
        }

        private boolean successful(int code) {
            if (read) {
                return code == OK_CODE || code == NOT_FOUND_CODE;
            }
            return code == CREATED_CODE || code == ACCEPTED_CODE;
        }

        private int successCode() {
            if (read) {
                return found ? OK_CODE : NOT_FOUND_CODE;
            }
            return PUT.equals(exchange.getRequestMethod()) ? CREATED_CODE : OK_CODE;
        }

        private void respond(int code) {
            done = true;
            try {
                finish(exchange, code, value);
            } catch (IOException e) {
                exchange.close();
            }
        }
    }
}
