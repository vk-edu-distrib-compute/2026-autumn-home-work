package company.vk.edu.distrib.compute.allpandasarecute.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

public class KVServiceImpl implements KVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_QUERY_PARAMETER = "id=";
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final int WORKER_THREADS = 4;

    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int INTERNAL_SERVER_ERROR = 500;

    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;

    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    private final int port;
    private final Dao<byte[]> dao;
    private final HttpServer server;
    private final ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.port = port;
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(WORKER_THREADS);
        this.server = HttpServer.create();
        server.setExecutor(executor);
        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
        server.createContext("/", this::handleUnknown);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service is already started");
        }
        try {
            server.bind(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Can not bind to port " + port, e);
        }
        server.start();
        log.info("KV service is listening on port {}", port);
    }

    @Override
    public void stop() {
        if (!started.getAndSet(false)) {
            return;
        }
        stopped.set(true);
        server.stop(1);
        executor.shutdownNow();
        log.info("KV service on port {} is stopped", port);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (GET_METHOD.equals(exchange.getRequestMethod())) {
                sendEmpty(exchange, OK);
            } else {
                sendEmpty(exchange, METHOD_NOT_ALLOWED);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                dispatchEntity(exchange);
            } catch (NoSuchElementException expected) {
                sendEmpty(exchange, NOT_FOUND);
            } catch (IOException | RuntimeException e) {
                log.error("Failed to handle entity request", e);
                sendEmpty(exchange, INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void dispatchEntity(HttpExchange exchange) throws IOException {
        String id = idFromQuery(exchange);
        if (id == null || id.isEmpty()) {
            sendEmpty(exchange, BAD_REQUEST);
            return;
        }
        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> sendBytes(exchange, OK, dao.get(id));
            case PUT_METHOD -> {
                dao.upsert(id, readBody(exchange));
                sendEmpty(exchange, CREATED);
            }
            case DELETE_METHOD -> {
                dao.delete(id);
                sendEmpty(exchange, ACCEPTED);
            }
            default -> sendEmpty(exchange, METHOD_NOT_ALLOWED);
        }
    }

    private void handleUnknown(HttpExchange exchange) throws IOException {
        try (exchange) {
            sendEmpty(exchange, NOT_FOUND);
        }
    }

    private static @Nullable String idFromQuery(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            if (pair.startsWith(ID_QUERY_PARAMETER)) {
                return URLDecoder.decode(pair.substring(ID_QUERY_PARAMETER.length()), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        try (var input = exchange.getRequestBody()) {
            return input.readAllBytes();
        }
    }

    private static void sendEmpty(HttpExchange exchange, int statusCode) throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
    }

    private static void sendBytes(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        if (body.length == 0) {
            exchange.sendResponseHeaders(statusCode, -1);
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", OCTET_STREAM);
        exchange.sendResponseHeaders(statusCode, body.length);
        try (var output = exchange.getResponseBody()) {
            output.write(body);
        }
    }
}
