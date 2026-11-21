package company.vk.edu.distrib.compute.netheer.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public final class KVServiceImplementation implements KVService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_QUERY_PREFIX = "id=";
    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final int WORKER_THREADS = 4;
    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_METHOD_NOT_ALLOWED = 405;
    private static final int HTTP_INTERNAL_SERVER_ERROR = 500;

    private final ExecutorService executor;

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImplementation(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        this.executor = Executors.newFixedThreadPool(WORKER_THREADS);
        server.setExecutor(executor);

        server.createContext(STATUS_PATH, this::handleStatus);
        server.createContext(ENTITY_PATH, this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        executor.close();

        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
                sendEmptyResponse(exchange, HTTP_NOT_FOUND);
                return;
            }

            if (!GET_METHOD.equals(exchange.getRequestMethod())) {
                sendEmptyResponse(exchange, HTTP_METHOD_NOT_ALLOWED);
                return;
            }

            sendEmptyResponse(exchange, HTTP_OK);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                processEntityRequest(exchange);
            } catch (NoSuchElementException e) {
                sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            } catch (IllegalArgumentException e) {
                sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
            } catch (IOException e) {
                sendEmptyResponse(exchange, HTTP_INTERNAL_SERVER_ERROR);
            }
        }
    }

    private void processEntityRequest(HttpExchange exchange) throws IOException {
        if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            sendEmptyResponse(exchange, HTTP_NOT_FOUND);
            return;
        }

        String key = extractKey(exchange);
        if (key == null) {
            sendEmptyResponse(exchange, HTTP_BAD_REQUEST);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> handleGet(exchange, key);
            case PUT_METHOD -> handlePut(exchange, key);
            case DELETE_METHOD -> handleDelete(exchange, key);
            default -> sendEmptyResponse(exchange, HTTP_METHOD_NOT_ALLOWED);
        }
    }

    private void handleGet(HttpExchange exchange, String key) throws IOException {
        byte[] value = dao.get(key);
        sendResponse(exchange, HTTP_OK, value);
    }

    private void handlePut(HttpExchange exchange, String key) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        dao.upsert(key, value);
        sendEmptyResponse(exchange, HTTP_CREATED);
    }

    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        dao.delete(key);
        sendEmptyResponse(exchange, HTTP_ACCEPTED);
    }

    private static String extractKey(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null || !query.startsWith(ID_QUERY_PREFIX) || query.contains("&")) {
            return null;
        }

        String encodedKey = query.substring(ID_QUERY_PREFIX.length());
        if (encodedKey.isEmpty()) {
            return null;
        }

        try {
            String key = URLDecoder.decode(encodedKey, StandardCharsets.UTF_8);
            return key.isEmpty() ? null : key;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static void sendEmptyResponse(HttpExchange exchange, int statusCode)
            throws IOException {
        exchange.sendResponseHeaders(statusCode, -1);
    }

    private static void sendResponse(
            HttpExchange exchange,
            int statusCode,
            byte[] body
    ) throws IOException {
        exchange.sendResponseHeaders(statusCode, body.length);

        try (OutputStream responseBody = exchange.getResponseBody()) {
            responseBody.write(body);
        }
    }
}
