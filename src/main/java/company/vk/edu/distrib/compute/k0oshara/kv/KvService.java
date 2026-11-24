package company.vk.edu.distrib.compute.k0oshara.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

final class KvService implements KVService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String REPLICA_HEADER = "X-K0oshara-Replica";
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final Set<String> ENTITY_METHODS = Set.of(GET, PUT, DELETE);
    private static final int CREATED = 0;
    private static final int STARTED = 1;
    private static final int STOPPED = 2;

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final Replication replication;
    private final ExecutorService executor;
    private final int port;
    private final ReentrantLock lifecycle = new ReentrantLock();
    private int state = CREATED;

    KvService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        server = HttpServer.create();
        server.createContext("/", new Handler());
        executor = createExecutor();
        if (executor != null) {
            server.setExecutor(executor);
        }
        this.port = port;
        replication = new Replication(port, dao);
    }

    @Override
    public void start() {
        lifecycle.lock();
        try {
            if (state != CREATED) {
                throw new IllegalStateException("Service was already started or stopped");
            }
            try {
                server.bind(new InetSocketAddress("localhost", port), 0);
                server.start();
                state = STARTED;
            } catch (IOException | RuntimeException exception) {
                state = STOPPED;
                closeResources();
                throw new IllegalStateException("Unable to start service", exception);
            }
        } finally {
            lifecycle.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycle.lock();
        try {
            if (state != STARTED) {
                throw new IllegalStateException("Service was not started or was already stopped");
            }
            state = STOPPED;
            server.stop(0);
            closeResources();
        } finally {
            lifecycle.unlock();
        }
    }

    private void closeResources() {
        try (dao; replication) {
            if (executor != null) {
                executor.shutdownNow();
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to close storage", exception);
        }
    }

    private static ExecutorService createExecutor() {
        String configured = System.getProperty("k0oshara.kv.threads", "");
        try {
            int threads = Integer.parseInt(configured);
            return threads > 1 ? Executors.newFixedThreadPool(threads) : null;
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private final class Handler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                handleRequest(exchange);
            } catch (IOException | RuntimeException exception) {
                send(exchange, 503, null);
            }
        }

        private void handleRequest(HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            URI uri = exchange.getRequestURI();
            if (STATUS_PATH.equals(uri.getPath())) {
                status(exchange, method);
                return;
            }
            if (!ENTITY_PATH.equals(uri.getPath())) {
                send(exchange, 404, null);
                return;
            }
            if (!ENTITY_METHODS.contains(method)) {
                send(exchange, 405, null);
                return;
            }
            String key;
            String ack;
            try {
                key = getParameter(uri, "id");
                ack = getParameter(uri, "ack");
            } catch (IllegalArgumentException exception) {
                send(exchange, 400, null);
                return;
            }
            if (key == null || key.isEmpty()) {
                send(exchange, 400, null);
                return;
            }
            byte[] body = PUT.equals(method) ? exchange.getRequestBody().readAllBytes() : null;
            boolean internal = "true".equalsIgnoreCase(
                exchange.getRequestHeaders().getFirst(REPLICA_HEADER));
            replication.execute(method, key, ack, body, internal)
                .whenComplete((reply, failure) -> sendReply(exchange, reply, failure));
        }

        private void sendReply(
            HttpExchange exchange,
            Replication.Reply reply,
            Throwable failure
        ) {
            try {
                if (failure != null || reply == null) {
                    send(exchange, 503, null);
                } else {
                    send(exchange, reply.status(), reply.body());
                }
            } catch (IOException exception) {
                throw new CompletionException(exception);
            }
        }

        private void status(HttpExchange exchange, String method) throws IOException {
            if (!GET.equals(method)) {
                send(exchange, 405, null);
                return;
            }
            try {
                dao.get("__status__");
            } catch (NoSuchElementException ignored) {
                send(exchange, 200, null);
                return;
            }
            send(exchange, 200, null);
        }

        private String getParameter(URI uri, String expectedName) {
            String query = uri.getRawQuery();
            if (query == null) {
                return null;
            }
            String value = null;
            for (String parameter : query.split("&", -1)) {
                int separator = parameter.indexOf('=');
                String name = decodeParameter(separator < 0
                    ? parameter : parameter.substring(0, separator));
                if (expectedName.equals(name)) {
                    if (value != null) {
                        throw new IllegalArgumentException("Duplicate query parameter");
                    }
                    value = decodeParameter(separator < 0
                        ? "" : parameter.substring(separator + 1));
                }
            }
            return value;
        }

        private String decodeParameter(String value) {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        }

        private void send(HttpExchange exchange, int status, byte[] body) throws IOException {
            try (exchange) {
                exchange.sendResponseHeaders(status, body == null ? -1 : body.length);
                if (body != null) {
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(body);
                    }
                }
            }
        }
    }
}
