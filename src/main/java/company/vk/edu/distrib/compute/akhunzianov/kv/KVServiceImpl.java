package company.vk.edu.distrib.compute.akhunzianov.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ENTITY_PREFIX = ENTITY_PATH + "/";
    private static final String ID_PARAM = "id=";
    private static final String PARAM_SEPARATOR = "&";

    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";

    private static final String OCTET_STREAM = "application/octet-stream";

    private static final int OK_CODE = 200;
    private static final int CREATED_CODE = 201;
    private static final int ACCEPTED_CODE = 202;
    private static final int BAD_REQUEST_CODE = 400;
    private static final int NOT_FOUND_CODE = 404;
    private static final int METHOD_NOT_ALLOWED_CODE = 405;
    private static final int INTERNAL_ERROR_CODE = 500;

    private static final int NO_BODY = -1;
    private static final int NOT_SENT = -1;

    private final int port;
    private final Dao<byte[]> dao;
    private final int threads;

    @Nullable
    private HttpServer server;

    @Nullable
    private ExecutorService workers;

    private boolean stopped;

    public KVServiceImpl(int port, Dao<byte[]> dao, int threads) {
        this.port = port;
        this.dao = dao;
        this.threads = threads;
    }

    @Override
    public void start() {
        if (server != null) {
            throw new IllegalStateException("Already started");
        }
        HttpServer http;
        try {
            http = HttpServer.create(new InetSocketAddress("localhost", port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot bind port " + port, e);
        }
        http.createContext(STATUS_PATH, guarded(exchange -> reply(exchange, OK_CODE)));
        http.createContext(ENTITY_PATH, guarded(this::onEntity));
        if (threads > 1) {
            workers = Executors.newFixedThreadPool(threads);
        }
        http.setExecutor(workers);
        http.start();
        server = http;
    }

    @Override
    public void stop() {
        if (server == null || stopped) {
            throw new IllegalStateException("Not started");
        }
        server.stop(0);
        if (workers != null) {
            workers.shutdown();
        }
        stopped = true;
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot close storage", e);
        }
    }

    private void onEntity(HttpExchange exchange) throws IOException {
        var key = keyOf(exchange.getRequestURI());
        if (key == null) {
            reply(exchange, NOT_FOUND_CODE);
            return;
        }
        if (key.isEmpty()) {
            reply(exchange, BAD_REQUEST_CODE);
            return;
        }
        switch (exchange.getRequestMethod()) {
            case GET -> read(exchange, key);
            case PUT -> write(exchange, key);
            case DELETE -> remove(exchange, key);
            default -> reply(exchange, METHOD_NOT_ALLOWED_CODE);
        }
    }

    private void read(HttpExchange exchange, String key) throws IOException {
        try {
            reply(exchange, OK_CODE, dao.get(key));
        } catch (NoSuchElementException e) {
            reply(exchange, NOT_FOUND_CODE);
        }
    }

    private void write(HttpExchange exchange, String key) throws IOException {
        dao.upsert(key, exchange.getRequestBody().readAllBytes());
        reply(exchange, CREATED_CODE);
    }

    private void remove(HttpExchange exchange, String key) throws IOException {
        dao.delete(key);
        reply(exchange, ACCEPTED_CODE);
    }

    @Nullable
    private static String keyOf(URI uri) {
        var path = uri.getPath();
        if (path.startsWith(ENTITY_PREFIX) && path.length() > ENTITY_PREFIX.length()) {
            return path.substring(ENTITY_PREFIX.length());
        }
        if (!ENTITY_PATH.equals(path) && !ENTITY_PREFIX.equals(path)) {
            return null;
        }
        return idParam(uri.getRawQuery());
    }

    private static String idParam(@Nullable String query) {
        if (query == null) {
            return "";
        }
        for (var param : query.split(PARAM_SEPARATOR)) {
            if (param.startsWith(ID_PARAM)) {
                return URLDecoder.decode(param.substring(ID_PARAM.length()), StandardCharsets.UTF_8);
            }
        }
        return "";
    }

    private static HttpHandler guarded(HttpHandler handler) {
        return exchange -> {
            try {
                handler.handle(exchange);
            } catch (RuntimeException | IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("Failed to handle {} {}", exchange.getRequestMethod(), exchange.getRequestURI(), e);
                }
                if (exchange.getResponseCode() == NOT_SENT) {
                    reply(exchange, INTERNAL_ERROR_CODE);
                } else {
                    exchange.close();
                }
            }
        };
    }

    private static void reply(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, NO_BODY);
        exchange.close();
    }

    private static void reply(HttpExchange exchange, int code, byte[] body) throws IOException {
        if (body.length == 0) {
            reply(exchange, code);
            return;
        }
        exchange.getResponseHeaders().add("Content-Type", OCTET_STREAM);
        exchange.sendResponseHeaders(code, body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
        exchange.close();
    }
}
