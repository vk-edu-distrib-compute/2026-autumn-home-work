package company.vk.edu.distrib.compute.ddkudrin.kv;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import company.vk.edu.distrib.compute.Dao;

public class DDKudrinEntityHandler implements HttpHandler {
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String INTERNAL_HEADER = "X-DDKudrin-Internal";
    private final Dao<byte[]> dao;
    private final ExecutorService coordinators = Executors.newVirtualThreadPerTaskExecutor();

    public DDKudrinEntityHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (System.getProperty("CLUSTER") != null && exchange.getRequestHeaders().getFirst(INTERNAL_HEADER) == null) {
            coordinators.execute(() -> {
                try {
                    handleExchange(exchange);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        } else {
            handleExchange(exchange);
        }
    }

    public void close() {
        coordinators.close();
    }

    private void handleExchange(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                route(exchange);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (IOException e) {
                if (exchange.getResponseCode() != -1) {
                    throw e;
                }
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    private void route(HttpExchange exchange) throws IOException {
        if (!Objects.equals(exchange.getRequestURI().getPath(), ENTITY_PATH)) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        String query = exchange.getRequestURI().getRawQuery();
        String key = readKey(query);
        String cluster = System.getProperty("CLUSTER");
        if (exchange.getRequestHeaders().getFirst(INTERNAL_HEADER) == null && cluster != null && !cluster.isBlank()) {
            new DDKudrinReplication(dao).replicate(exchange, key, query, cluster);
            return;
        }
        if (exchange.getRequestHeaders().getFirst(INTERNAL_HEADER) == null) {
            readAck(query, 1);
        }
        if (Objects.equals(exchange.getRequestMethod(), "GET")) {
            byte[] value = dao.get(key);
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, value.length == 0 ? -1 : value.length);
            if (value.length != 0) {
                exchange.getResponseBody().write(value);
            }
        } else if (Objects.equals(exchange.getRequestMethod(), "PUT")) {
            dao.upsert(key, exchange.getRequestBody().readAllBytes());
            exchange.sendResponseHeaders(201, -1);
        } else if (Objects.equals(exchange.getRequestMethod(), "DELETE")) {
            dao.delete(key);
            exchange.sendResponseHeaders(202, -1);
        } else {
            exchange.sendResponseHeaders(405, -1);
        }
    }

    static int readAck(String query, int replicas) {
        int ack = 1;
        boolean found = false;
        for (String parameter : query.split("&")) {
            String[] parts = parameter.split("=", 2);
            if (Objects.equals(URLDecoder.decode(parts[0], StandardCharsets.UTF_8), "ack")) {
                if (found || parts.length != 2) {
                    throw new IllegalArgumentException("Invalid ack parameter");
                }
                found = true;
                ack = Integer.parseInt(URLDecoder.decode(parts[1], StandardCharsets.UTF_8));
            }
        }
        if (ack < 1 || ack > replicas) {
            throw new IllegalArgumentException("Invalid ack parameter");
        }
        return ack;
    }

    private static String readKey(String query) {
        if (query == null) {
            throw new IllegalArgumentException("Missing query");
        }
        String key = null;
        for (String parameter : query.split("&")) {
            String[] parts = parameter.split("=", 2);
            if (Objects.equals(URLDecoder.decode(parts[0], StandardCharsets.UTF_8), "id")) {
                if (key != null || parts.length != 2) {
                    throw new IllegalArgumentException("Invalid id parameter");
                }
                key = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
            }
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        return key;
    }
}

