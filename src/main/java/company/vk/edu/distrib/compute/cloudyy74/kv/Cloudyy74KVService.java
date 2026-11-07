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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Cloudyy74KVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(Cloudyy74KVService.class);

    private final int port;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Cloudyy74PersistentDao dao;
    private final Cloudyy74Replication replication;

    private static final int QUERY_PARAMETER_PARTS = 2;
    private static final int MIN_ACK = 1;
    private static final String ID_PARAMETER = "id";
    private static final String ACK_PARAMETER = "ack";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";

    private static final String GET_METHOD = "GET";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final Set<String> ENTITY_METHODS = Set.of(GET_METHOD, PUT_METHOD, DELETE_METHOD);

    public Cloudyy74KVService(int port) throws IOException {
        this.port = port;
        this.dao = new Cloudyy74PersistentDao("./out/kv-%d.log".formatted(port));
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.replication = new Cloudyy74Replication(port, dao);
        server.setExecutor(executor);
        server.createContext(STATUS_PATH, new ErrorHandler(this::handleStatus));
        server.createContext(ENTITY_PATH, new ErrorHandler(this::handleEntity));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        executor.close();
        replication.close();
        dao.close();
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(dao.isHealthy() ? 200 : 503, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }
        final var parameters = parseQuery(exchange);
        final var id = parseId(parameters);
        final var config = Cloudyy74ClusterConfig.fromProperties(port);
        final var ack = parseAck(parameters, config.replicationFactor());
        final var method = exchange.getRequestMethod();
        if (!ENTITY_METHODS.contains(method)) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        final var body = PUT_METHOD.equals(method) ? exchange.getRequestBody().readAllBytes() : new byte[0];
        final var internal = Cloudyy74Replication.REPLICA_HEADER_VALUE.equals(
                exchange.getRequestHeaders().getFirst(Cloudyy74Replication.REPLICA_HEADER));
        final var response = internal || !config.replicated()
                ? replication.local(method, id, body) : replication.coordinate(method, id, body, config, ack);
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(response.status(), response.body().length == 0 ? -1 : response.body().length);
        if (response.body().length > 0) {
            exchange.getResponseBody().write(response.body());
        }
    }

    private static String parseId(Map<String, String> parameters) {
        final var id = parameters.getOrDefault(ID_PARAMETER, "");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("Missing or empty ID");
        }
        return id;
    }

    private static int parseAck(Map<String, String> parameters, int replicationFactor) {
        final var ack = Integer.parseInt(parameters.getOrDefault(ACK_PARAMETER, Integer.toString(MIN_ACK)));
        if (ack < MIN_ACK || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack");
        }
        return ack;
    }

    private static Map<String, String> parseQuery(HttpExchange exchange) {
        final var query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            throw new IllegalArgumentException("Missing query parameters");
        }
        final Map<String, String> parameters = new ConcurrentHashMap<>();
        for (String parameter : query.split("&")) {
            final var parts = parameter.split("=", QUERY_PARAMETER_PARTS);
            final var name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            final var value = parts.length == QUERY_PARAMETER_PARTS
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            parameters.putIfAbsent(name, value);
        }
        return parameters;
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
