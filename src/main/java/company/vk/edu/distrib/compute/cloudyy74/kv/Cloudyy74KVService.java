package company.vk.edu.distrib.compute.cloudyy74.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Cloudyy74KVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(Cloudyy74KVService.class);

    private final HttpServer server;
    private final Optional<ExecutorService> executor;
    private final Cloudyy74PersistentDao dao = new Cloudyy74PersistentDao("./out/kv.log");

    private static final int MIN_WORKER_THREADS = 1;
    private static final int QUERY_PARAMETER_PARTS = 2;
    private static final String ID_PARAMETER = "id";

    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";

    public Cloudyy74KVService(int port) throws IOException {
        final var threads = Integer.getInteger("kv.threads", MIN_WORKER_THREADS);
        if (threads < MIN_WORKER_THREADS) {
            throw new IllegalArgumentException("Invalid worker thread count");
        }
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = threads > MIN_WORKER_THREADS
                ? Optional.of(Executors.newFixedThreadPool(threads)) : Optional.empty();
        executor.ifPresent(server::setExecutor);
        server.createContext("/v0/status", new ErrorHandler(this::handleStatus));
        server.createContext("/v0/entity", new ErrorHandler(this::handleEntity));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
        dao.close();
        executor.ifPresent(ExecutorService::close);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(dao.isHealthy() ? 200 : 503, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        final var id = parseId(exchange);
        switch (exchange.getRequestMethod()) {
            case GET_METHOD -> {
                final var body = dao.get(id);
                exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
                exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
                exchange.getResponseBody().write(body);
            }
            case PUT_METHOD -> {
                dao.upsert(id, exchange.getRequestBody().readAllBytes());
                exchange.sendResponseHeaders(201, -1);
            }
            case DELETE_METHOD -> {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
            }
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private static String parseId(HttpExchange exchange) {
        final var query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            throw new IllegalArgumentException("Missing ID");
        }
        final var parts = Arrays.stream(query.split("&"))
                .map(parameter -> parameter.split("=", QUERY_PARAMETER_PARTS))
                .filter(parameter -> ID_PARAMETER.equals(URLDecoder.decode(parameter[0], StandardCharsets.UTF_8)))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Missing ID"));
        if (parts.length != QUERY_PARAMETER_PARTS || parts[1].isEmpty()) {
            throw new IllegalArgumentException("Empty ID");
        }
        return URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
    }

    private record ErrorHandler(HttpHandler delegate) implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(400, -1);
                } catch (IOException e) {
                    log.error("I/O error while processing HTTP request", e);
                    exchange.sendResponseHeaders(503, -1);
                } catch (Exception e) {
                    log.error("Unexpected error while processing HTTP request", e);
                    exchange.sendResponseHeaders(500, -1);
                }
            }
        }
    }
}
