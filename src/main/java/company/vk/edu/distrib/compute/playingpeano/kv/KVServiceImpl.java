package company.vk.edu.distrib.compute.playingpeano.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jspecify.annotations.Nullable;

final class KVServiceImpl implements KVService {
    private static final String WORKERS_ENVIRONMENT_VARIABLE = "KV_WORKERS";
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String GET_METHOD = "GET";
    private static final int STOP_DELAY_SECONDS = 1;

    private final PersistentByteArrayDao dao;
    private final ReplicaClient replicaClient;
    private final RequestCoordinator coordinator;
    private final HttpServer server;
    private final @Nullable ExecutorService executor;

    KVServiceImpl(int port, PersistentByteArrayDao dao) throws IOException {
        this.dao = dao;
        replicaClient = new ReplicaClient();
        coordinator = new RequestCoordinator(port, dao, replicaClient);
        try {
            server = HttpServer.create(new InetSocketAddress(bindHost(), port), 0);
            server.createContext("/", this::handle);
            executor = createExecutor();
            if (executor != null) {
                server.setExecutor(executor);
            }
        } catch (IOException | RuntimeException | Error exception) {
            replicaClient.close();
            throw exception;
        }
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try (dao; replicaClient) {
            server.stop(STOP_DELAY_SECONDS);
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    private static String bindHost() {
        @Nullable String configuredHost = System.getenv("KV_BIND");
        if (configuredHost == null || configuredHost.isBlank()) {
            return "localhost";
        }
        return configuredHost;
    }

    private static @Nullable ExecutorService createExecutor() {
        @Nullable String configuredWorkers = System.getenv(WORKERS_ENVIRONMENT_VARIABLE);
        if (configuredWorkers == null || configuredWorkers.isBlank()) {
            return null;
        }
        int workers = Integer.parseInt(configuredWorkers);
        if (workers <= 0) {
            throw new IllegalArgumentException("KV_WORKERS must be positive");
        }
        return Executors.newFixedThreadPool(workers);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                route(exchange);
            } catch (NoSuchElementException exception) {
                HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_NOT_FOUND);
            } catch (IllegalArgumentException exception) {
                HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_BAD_REQUEST);
            } catch (IOException exception) {
                HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_UNAVAILABLE);
            } catch (RuntimeException exception) {
                HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_SERVER_ERROR);
            }
        }
    }

    private void route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (STATUS_PATH.equals(path)) {
            handleStatus(exchange);
        } else if (ENTITY_PATH.equals(path)) {
            coordinator.handle(exchange, EntityQuery.parse(exchange.getRequestURI().getRawQuery()));
        } else {
            HttpResponses.sendEmpty(exchange, HttpResponses.STATUS_NOT_FOUND);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            HttpResponses.sendMethodNotAllowed(exchange, GET_METHOD);
            return;
        }
        HttpResponses.sendEmpty(
            exchange,
            dao.isAvailable() ? HttpResponses.STATUS_OK : HttpResponses.STATUS_UNAVAILABLE
        );
    }
}
