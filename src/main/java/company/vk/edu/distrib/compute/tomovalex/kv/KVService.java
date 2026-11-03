package company.vk.edu.distrib.compute.tomovalex.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVService implements company.vk.edu.distrib.compute.kv.KVService {
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final int THREADS_COUNT = 8;

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final ExecutorService executor;
    private final ExecutorService clusterExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final int replicationFactor;
    private final ReplicaSelector replicaSelector;
    private final ReplicationCoordinator coordinator;

    public KVService(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, false);
    }

    public KVService(int port, Dao<byte[]> dao, boolean multithreaded) throws IOException {
        this(port, dao, multithreaded, 1, port);
    }

    public KVService(int port, Dao<byte[]> dao, boolean multithreaded,
                     int replicationFactor, int... ports) throws IOException {
        this.replicaSelector = new ReplicaSelector(ports);
        if (!replicaSelector.getPorts().contains(port)) {
            throw new IllegalArgumentException("Local port is missing from cluster: " + port);
        }
        if (replicationFactor < 1 || replicationFactor > replicaSelector.getPorts().size()) {
            throw new IllegalArgumentException("Invalid replication factor: " + replicationFactor);
        }
        this.replicationFactor = replicationFactor;
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = Executors.newFixedThreadPool(multithreaded ? THREADS_COUNT : 1);
        this.coordinator = new ReplicationCoordinator(port, dao);
        server.setExecutor(command -> {
            if (System.getProperty("CLUSTER") != null || replicaSelector.getPorts().size() > 1) {
                clusterExecutor.execute(command);
            } else {
                executor.execute(command);
            }
        });
        server.createContext("/", this::handleRequest);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        executor.close();
        clusterExecutor.close();
        coordinator.close();
        try {
            dao.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if (STATUS_PATH.equals(path)) {
                sendEmptyResponse(exchange, GET.equals(exchange.getRequestMethod()) ? 200 : 405);
            } else if (ENTITY_PATH.equals(path)) {
                handleEntity(exchange);
            } else {
                sendEmptyResponse(exchange, 404);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try {
            String id = getParameter(exchange, "id");
            if (id == null || id.isEmpty()) {
                sendEmptyResponse(exchange, 400);
                return;
            }
            List<Integer> replicas = getReplicas(id);
            String ackParameter = getParameter(exchange, "ack");
            int ack = ackParameter == null ? 1 : Integer.parseInt(ackParameter);
            if (ack < 1 || ack > replicas.size()) {
                sendEmptyResponse(exchange, 400);
                return;
            }
            handleEntityMethod(exchange, id, replicas, ack);
        } catch (IllegalArgumentException e) {
            sendEmptyResponse(exchange, 400);
        }
    }

    private List<Integer> getReplicas(String id) {
        String cluster = System.getProperty("CLUSTER");
        if (cluster == null) {
            return replicaSelector.selectReplicas(id, replicationFactor);
        }
        int[] ports = Arrays.stream(cluster.split(",", -1)).map(String::trim).mapToInt(Integer::parseInt).toArray();
        ReplicaSelector selector = new ReplicaSelector(ports);
        return selector.selectReplicas(id, selector.getPorts().size());
    }

    private void handleEntityMethod(HttpExchange exchange, String id, List<Integer> replicas, int ack)
        throws IOException {
        String method = exchange.getRequestMethod();
        if (!GET.equals(method) && !PUT.equals(method) && !DELETE.equals(method)) {
            sendEmptyResponse(exchange, 405);
            return;
        }
        byte[] body = PUT.equals(method) ? exchange.getRequestBody().readAllBytes() : new byte[0];
        ReplicationCoordinator.Response response;
        String replicaHeader = exchange.getRequestHeaders().getFirst(ReplicationCoordinator.REPLICA_HEADER);
        if (Objects.equals(replicaHeader, "true")) {
            response = coordinator.executeLocal(method, id, body);
        } else {
            response = coordinator.execute(method, id, body, replicas, ack);
        }
        if (GET.equals(method) && response.status() == 200) {
            sendResponse(exchange, response.body());
        } else {
            sendEmptyResponse(exchange, response.status());
        }
    }

    private static String getParameter(HttpExchange exchange, String parameterName) {
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) {
            return null;
        }
        for (String parameter : query.split("&")) {
            String[] parts = parameter.split("=", 2);
            String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            if (Objects.equals(name, parameterName)) {
                return parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            }
        }
        return null;
    }

    private static void sendResponse(HttpExchange exchange, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }
}
