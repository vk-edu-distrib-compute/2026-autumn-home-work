package company.vk.edu.distrib.compute.dzolin.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVServiceImpl implements company.vk.edu.distrib.compute.kv.KVService {
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private final HttpServer server;
    private final ExecutorService workers;
    private final PersistentDao dao;

    public KVServiceImpl(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        workers = Executors.newFixedThreadPool(1);
        server.setExecutor(workers);

        dao = new PersistentDao(Path.of(System.getProperty("java.io.tmpdir"), "dzolin-kv-" + port + ".dat").toString());

        server.createContext("/v0/status", new ErrorHandler(this::getStatus));
        server.createContext("/v0/entity", new ErrorHandler(this::handleEntity));
    }

    public void getStatus(HttpExchange exchange) throws IOException {
        if (!GET.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }

    public void handleEntity(HttpExchange exchange) throws IOException {
        var method = exchange.getRequestMethod();
        if (!Set.of(GET, PUT, DELETE).contains(method)) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        var id = getQueryParameter(exchange, "id");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("entity id must be present");
        }

        switch (exchange.getRequestMethod()) {
            case "GET" -> sendResponse(exchange, 200, dao.get(id));
            case "PUT" -> {
                dao.upsert(id, exchange.getRequestBody().readAllBytes());
                sendResponse(exchange, 201, new byte[0]);
            }
            default -> {
                dao.delete(id);
                sendResponse(exchange, 202, new byte[0]);
            }
        }
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, byte[] data) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        if (data.length == 0) {
            exchange.sendResponseHeaders(statusCode, -1);
            return;
        }
        exchange.sendResponseHeaders(statusCode, data.length);
        exchange.getResponseBody().write(data);
    }

    private static String getQueryParameter(HttpExchange exchange, String name) {
        var query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return "";
        }

        for (var parameter : query.split("&")) {
            var parts = parameter.split("=", 2);
            var key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);

            if (key.equals(name)) {
                return parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            }
        }
        return "";
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        try (dao) {
            server.stop(1);
            workers.shutdown();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
