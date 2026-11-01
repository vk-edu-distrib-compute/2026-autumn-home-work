package company.vk.edu.distrib.compute.fedorkhokhryakov.kv;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicatingEntityHandler implements EntityHandler {
    private static final Logger log = LoggerFactory.getLogger(ReplicatingEntityHandler.class);

    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String REPLICA_HEADER = "X-Replica-Request";
    private static final String ACK_PARAM = "ack";
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";

    private static final Duration REPLICA_TIMEOUT = Duration.ofSeconds(1);
    private static final long QUORUM_TIMEOUT_MILLIS = REPLICA_TIMEOUT.toMillis() + 200;

    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;
    private static final int SERVICE_UNAVAILABLE = 503;
    private static final int NO_BODY = -1;
    private static final int DEFAULT_ACK = 1;

    private final int selfPort;
    private final int replicationFactor;
    private final Dao<byte[]> dao;
    private final LocalEntityHandler local;
    private final HttpClient client;
    private final AtomicReference<@Nullable ClusterTopology> topology = new AtomicReference<>();

    public ReplicatingEntityHandler(int selfPort, Dao<byte[]> dao, int replicationFactor) {
        this.selfPort = selfPort;
        this.replicationFactor = replicationFactor;
        this.dao = dao;
        this.local = new LocalEntityHandler(dao);
        this.client = HttpClient.newBuilder()
            .connectTimeout(REPLICA_TIMEOUT)
            .build();
    }

    @Override
    public void handle(HttpExchange exchange, String id) throws IOException {
        if (exchange.getRequestHeaders().containsKey(REPLICA_HEADER)) {
            local.handle(exchange, id);
            return;
        }

        String method = exchange.getRequestMethod();
        if (!GET.equals(method) && !PUT.equals(method) && !DELETE.equals(method)) {
            exchange.sendResponseHeaders(METHOD_NOT_ALLOWED, NO_BODY);
            return;
        }

        ClusterTopology cluster = topology();
        int replicas = replicationFactor > 0 ? Math.min(replicationFactor, cluster.size()) : cluster.size();
        int ack = ack(exchange, replicas);
        if (ack < DEFAULT_ACK) {
            exchange.sendResponseHeaders(BAD_REQUEST, NO_BODY);
            return;
        }

        byte[] body = PUT.equals(method) ? exchange.getRequestBody().readAllBytes() : new byte[0];
        Quorum quorum = new Quorum(ack, replicas);
        int[] owners = cluster.replicas(id, replicas);
        boolean selfIsReplica = false;
        for (int port : owners) {
            if (port == selfPort) {
                selfIsReplica = true;
            } else {
                sendToReplica(port, method, id, body, quorum);
            }
        }
        if (selfIsReplica) {
            applyLocally(method, id, body, quorum);
        }

        respond(exchange, method, quorum);
    }

    @Override
    public void close() throws IOException {
        client.close();
        local.close();
    }

    private ClusterTopology topology() {
        String description = System.getProperty(CLUSTER_PROPERTY, "");
        ClusterTopology current = topology.get();
        if (current == null || !current.describedBy(description)) {
            current = ClusterTopology.parse(description, selfPort);
            topology.set(current);
        }
        return current;
    }

    private static int ack(HttpExchange exchange, int replicas) {
        String value;
        try {
            value = QueryParams.find(exchange.getRequestURI().getRawQuery(), ACK_PARAM);
        } catch (IllegalArgumentException e) {
            return -1;
        }
        if (value == null) {
            return DEFAULT_ACK;
        }
        try {
            int ack = Integer.parseInt(value);
            return ack <= replicas ? ack : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void sendToReplica(int port, String method, String id, byte[] body, Quorum quorum) {
        URI uri = URI.create("http://localhost:" + port + "/v0/entity?id=" + URLEncoder.encode(id, StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(REPLICA_TIMEOUT)
            .header(REPLICA_HEADER, "1")
            .method(method, PUT.equals(method)
                ? HttpRequest.BodyPublishers.ofByteArray(body)
                : HttpRequest.BodyPublishers.noBody())
            .build();
        client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .whenComplete((response, error) -> {
                if (error != null) {
                    quorum.failure();
                } else {
                    acceptReplicaResponse(method, response, quorum);
                }
            });
    }

    private static void acceptReplicaResponse(String method, HttpResponse<byte[]> response, Quorum quorum) {
        int status = response.statusCode();
        if (GET.equals(method) && status == OK) {
            quorum.success(response.body());
        } else if (GET.equals(method) && status == NOT_FOUND
            || PUT.equals(method) && status == CREATED
            || DELETE.equals(method) && status == ACCEPTED) {
            quorum.success(null);
        } else {
            quorum.failure();
        }
    }

    private void applyLocally(String method, String id, byte[] body, Quorum quorum) {
        try {
            switch (method) {
                case GET -> quorum.success(findLocally(id));
                case PUT -> {
                    dao.upsert(id, body);
                    quorum.success(null);
                }
                default -> {
                    dao.delete(id);
                    quorum.success(null);
                }
            }
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("Local replica failed to process {} for key {}", method, id, e);
            }
            quorum.failure();
        }
    }

    private byte @Nullable [] findLocally(String id) throws IOException {
        try {
            return dao.get(id);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private static void respond(HttpExchange exchange, String method, Quorum quorum) throws IOException {
        if (!quorum.await(QUORUM_TIMEOUT_MILLIS)) {
            exchange.sendResponseHeaders(SERVICE_UNAVAILABLE, NO_BODY);
            return;
        }
        switch (method) {
            case GET -> {
                byte[] value = quorum.value();
                if (value == null) {
                    exchange.sendResponseHeaders(NOT_FOUND, NO_BODY);
                } else {
                    LocalEntityHandler.sendBody(exchange, OK, value);
                }
            }
            case PUT -> exchange.sendResponseHeaders(CREATED, NO_BODY);
            default -> exchange.sendResponseHeaders(OK, NO_BODY);
        }
    }
}
