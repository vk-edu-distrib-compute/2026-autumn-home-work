package company.vk.edu.distrib.compute.artyompeshkov.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVServiceImpl implements KVService {
    private static final int MAX_VALUE_SIZE = 1024 * 1024;
    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private final BitcaskDao dao;
    private final ReplicaCoordinator coordinator;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final HttpServer server;

    public KVServiceImpl(int port, BitcaskDao dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.coordinator = new ReplicaCoordinator(port, dao);
        server.setExecutor(executor);
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", exchange -> handleEntity(exchange, true));
        server.createContext("/internal/entity", exchange -> handleEntity(exchange, false));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        executor.shutdownNow();
        coordinator.close();
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close the storage", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            int code = dao.isAvailable() ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_UNAVAILABLE;
            exchange.sendResponseHeaders(code, -1);
        }
    }

    private void handleEntity(HttpExchange exchange, boolean coordinate) throws IOException {
        try (exchange) {
            try {
                String method = exchange.getRequestMethod();
                if (!"GET".equals(method) && !"PUT".equals(method) && !"DELETE".equals(method)) {
                    exchange.getResponseHeaders().set("Allow", "GET, PUT, DELETE");
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                    return;
                }
                String query = exchange.getRequestURI().getRawQuery();
                String id = param(query, "id", "");
                if (id.isEmpty()) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                    return;
                }
                List<Integer> replicas = coordinate ? coordinator.replicasOf(id) : List.of();
                int ack = parseAck(param(query, "ack", "1"), Math.max(1, replicas.size()));
                serveRecord(exchange, id, ack, replicas);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
            } catch (IOException e) {
                if (exchange.getResponseCode() != -1) {
                    throw e;
                }
                log.error("Failed to handle request", e);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, -1);
            }
        }
    }

    private void serveRecord(HttpExchange exchange, String id, int ack, List<Integer> replicas) throws IOException {
        String method = exchange.getRequestMethod();
        byte[] value = "PUT".equals(method) ? readValue(exchange) : new byte[0];
        byte[] result;
        try {
            result = replicas.isEmpty()
                    ? coordinator.applyLocally(method, id, value)
                    : coordinator.process(method, id, value, ack, replicas);
        } catch (IOException e) {
            log.error("Storage operation failed", e);
            exchange.sendResponseHeaders(replicas.isEmpty()
                    ? HttpURLConnection.HTTP_INTERNAL_ERROR : HttpURLConnection.HTTP_UNAVAILABLE, -1);
            return;
        }
        switch (method) {
            case "GET" -> sendValue(exchange, result);
            case "PUT" -> exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
            case "DELETE" -> exchange.sendResponseHeaders(replicas.isEmpty()
                    ? HttpURLConnection.HTTP_ACCEPTED : HttpURLConnection.HTTP_OK, -1);
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        }
    }

    private static int parseAck(String ack, int replicas) {
        int result = Integer.parseInt(ack);
        if (result < 1 || result > replicas) {
            throw new IllegalArgumentException("ack must be between 1 and " + replicas);
        }
        return result;
    }

    private static String param(@Nullable String query, String name, String fallback) {
        if (query == null) {
            return fallback;
        }
        for (String param : query.split("&")) {
            String[] parts = param.split("=", 2);
            if (URLDecoder.decode(parts[0], StandardCharsets.UTF_8).equals(name)) {
                return parts.length == 1 ? "" : URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
            }
        }
        return fallback;
    }

    private static byte[] readValue(HttpExchange exchange) throws IOException {
        byte[] value = exchange.getRequestBody().readNBytes(MAX_VALUE_SIZE + 1);
        if (value.length > MAX_VALUE_SIZE) {
            throw new IllegalArgumentException("Value is too large");
        }
        return value;
    }

    private static void sendValue(HttpExchange exchange, byte[] value) throws IOException {
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, value.length == 0 ? -1 : value.length);
        exchange.getResponseBody().write(value);
    }
}
