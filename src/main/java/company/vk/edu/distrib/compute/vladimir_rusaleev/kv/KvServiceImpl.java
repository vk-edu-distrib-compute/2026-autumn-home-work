package company.vk.edu.distrib.compute.vladimir_rusaleev.kv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

final class KvServiceImpl implements KVService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String ID_PARAMETER = "id=";
    private static final String GET_METHOD = "GET";
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final int SINGLE_THREAD = 1;

    private final int port;
    private final int threads;
    private final Dao<byte[]> data;
    private HttpServer server;
    private ExecutorService executor;

    KvServiceImpl(int port, int threads) throws IOException {
        if (threads < SINGLE_THREAD) {
            throw new IllegalArgumentException("Thread count must be positive");
        }
        this.port = port;
        this.threads = threads;
        String defaultDirectory = Path.of(System.getProperty("user.home"), ".vk-kv", "vladimir").toString();
        Path root = Path.of(System.getenv().getOrDefault("KV_DATA_DIR", defaultDirectory));
        data = new BinaryFileDao(root.resolve(Integer.toString(port)));
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
            server.createContext("/", this::handle);
            if (threads > SINGLE_THREAD) {
                executor = Executors.newFixedThreadPool(threads);
                server.setExecutor(executor);
            }
            server.start();
        } catch (IOException exception) {
            throw new UncheckedIOException("Could not start KV service", exception);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.close();
        }
        try {
            data.close();
        } catch (IOException exception) {
            throw new UncheckedIOException("Could not close KV storage", exception);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                route(exchange);
            } catch (IllegalArgumentException exception) {
                respond(exchange, 400, EMPTY_BODY);
            } catch (IOException exception) {
                respond(exchange, 503, EMPTY_BODY);
            }
        }
    }

    private void route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (STATUS_PATH.equals(path)) {
            int status = GET_METHOD.equals(exchange.getRequestMethod()) ? 200 : 405;
            respond(exchange, status, EMPTY_BODY);
            return;
        }
        if (!ENTITY_PATH.equals(path)) {
            respond(exchange, 404, EMPTY_BODY);
            return;
        }
        String key = readKey(exchange);
        switch (exchange.getRequestMethod()) {
            case "GET" -> get(exchange, key);
            case "PUT" -> {
                data.upsert(key, exchange.getRequestBody().readAllBytes());
                respond(exchange, 201, EMPTY_BODY);
            }
            case "DELETE" -> {
                data.delete(key);
                respond(exchange, 202, EMPTY_BODY);
            }
            default -> respond(exchange, 405, EMPTY_BODY);
        }
    }

    private void get(HttpExchange exchange, String key) throws IOException {
        try {
            respond(exchange, 200, data.get(key));
        } catch (NoSuchElementException exception) {
            respond(exchange, 404, EMPTY_BODY);
        }
    }

    private static String readKey(HttpExchange exchange) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            throw new IllegalArgumentException("Missing id");
        }
        String key = null;
        for (String parameter : query.split("&")) {
            if (parameter.startsWith(ID_PARAMETER)) {
                if (key != null) {
                    throw new IllegalArgumentException("Duplicate id");
                }
                key = URLDecoder.decode(parameter.substring(ID_PARAMETER.length()), StandardCharsets.UTF_8);
            }
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Empty id");
        }
        return key;
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
        }
    }
}
