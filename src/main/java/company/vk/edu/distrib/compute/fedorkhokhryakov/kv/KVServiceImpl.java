package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAM = "id";

    private static final int OK = 200;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int INTERNAL_ERROR = 500;
    private static final int NO_BODY = -1;

    private static final int MIN_THREADS = 1;

    private enum State {
        NEW,
        STARTED,
        STOPPED
    }

    private final int port;
    private final EntityHandler handler;
    private final HttpServer server;
    private final ExecutorService executor;
    private State state = State.NEW;

    public KVServiceImpl(int port, Dao<byte[]> dao, int threads) throws IOException {
        this(port, new LocalEntityHandler(dao), fixedPool(threads));
    }

    public KVServiceImpl(int port, EntityHandler handler, ExecutorService executor) throws IOException {
        this.port = port;
        this.handler = handler;
        this.server = HttpServer.create();
        this.executor = executor;
        server.setExecutor(executor);
        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
    }

    @Override
    public void start() {
        if (state != State.NEW) {
            throw new IllegalStateException("Service can be started only once");
        }

        try {
            server.bind(new InetSocketAddress("localhost", port), 0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start HTTP server on port " + port, e);
        }
        server.start();
        state = State.STARTED;
    }

    @Override
    public void stop() {
        if (state != State.STARTED) {
            throw new IllegalStateException("Service has not been started or is already stopped");
        }

        server.stop(0);
        executor.shutdownNow();
        state = State.STOPPED;
        try {
            handler.close();
        } catch (IOException e) {
            log.warn("Failed to close entity handler", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            int status;
            if (STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
                status = "GET".equals(exchange.getRequestMethod()) ? OK : METHOD_NOT_ALLOWED;
            } else {
                status = NOT_FOUND;
            }
            exchange.sendResponseHeaders(status, NO_BODY);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(NOT_FOUND, NO_BODY);
                return;
            }

            String id = requestId(exchange);
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(BAD_REQUEST, NO_BODY);
                return;
            }

            try {
                handler.handle(exchange, id);
            } catch (IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("Failed to process {} request for key {}", exchange.getRequestMethod(), id, e);
                }
                exchange.sendResponseHeaders(INTERNAL_ERROR, NO_BODY);
            }
        }
    }

    private static @Nullable String requestId(HttpExchange exchange) {
        try {
            return QueryParams.find(exchange.getRequestURI().getRawQuery(), ID_PARAM);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static ExecutorService fixedPool(int threads) {
        if (threads < MIN_THREADS) {
            throw new IllegalArgumentException("Threads count must be positive: " + threads);
        }
        return Executors.newFixedThreadPool(threads);
    }
}
