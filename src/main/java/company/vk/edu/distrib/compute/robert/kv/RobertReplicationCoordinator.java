package company.vk.edu.distrib.compute.robert.kv;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.robert.api.models.BadRequestException;
import company.vk.edu.distrib.compute.robert.api.models.HttpStatus;
import company.vk.edu.distrib.compute.robert.api.models.Request;
import company.vk.edu.distrib.compute.robert.api.models.Response;
import company.vk.edu.distrib.compute.robert.kv.api.v0.EntityHandler;
import company.vk.edu.distrib.compute.robert.kv.replication.ReplicationCluster;
import company.vk.edu.distrib.compute.robert.kv.replication.ReplicationQuorum;

public class RobertReplicationCoordinator implements Closeable {
    public static final String REPLICA_HEADER = "X-Robert-Replica";
    private static final String CLUSTER = "CLUSTER";
    private static final String REPLICATION_FACTOR = "REPLICATION_FACTOR";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);

    private final int port;
    private final EntityHandler localHandler;
    private final HttpClient client;
    private final AtomicReference<ReplicationCluster> cluster = new AtomicReference<>();

    public RobertReplicationCoordinator(int inputPort, Dao<byte[]> inputDao) {
        port = inputPort;
        localHandler = new EntityHandler(inputDao);
        client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(REQUEST_TIMEOUT)
            .build();
    }

    public CompletableFuture<Response> replicate(Request request, String key, String inputAck) throws IOException {
        ReplicationCluster currentCluster = getCluster();
        int ack = parseAck(inputAck, currentCluster.replicationFactor());
        int successStatus = switch (request.method()) {
            case "GET" -> HttpStatus.NOT_FOUND.code();
            case "PUT" -> HttpStatus.CREATED.code();
            case "DELETE" -> currentCluster.replicationFactor() == 1
                ? HttpStatus.ACCEPTED.code() : HttpStatus.OK.code();
            default -> throw new IllegalArgumentException();
        };
        ReplicationQuorum quorum = new ReplicationQuorum(ack, currentCluster.replicationFactor(), successStatus);
        List<Integer> replicas = currentCluster.replicas(key);

        for (int replicaPort : replicas) {
            if (replicaPort != port) {
                sendToReplica(replicaPort, key, request, quorum);
            }
        }
        if (replicas.contains(port)) {
            quorum.add(handleLocal(request));
        }
        return quorum.result();
    }

    @Override
    public void close() {
        client.shutdownNow();
    }

    private ReplicationCluster getCluster() throws IOException {
        String nodes = configuration(CLUSTER, Integer.toString(port));
        String factor = configuration(REPLICATION_FACTOR, "");
        ReplicationCluster currentCluster = cluster.get();
        if (currentCluster == null || !currentCluster.matches(nodes, factor)) {
            try {
                currentCluster = new ReplicationCluster(nodes, factor);
            } catch (IllegalArgumentException e) {
                throw new IOException(e);
            }
            cluster.set(currentCluster);
        }
        return currentCluster;
    }

    private static String configuration(String name, String defaultValue) {
        return System.getProperty(name, System.getenv().getOrDefault(name, defaultValue));
    }

    private static int parseAck(String value, int replicationFactor) {
        if (value == null) {
            return 1;
        }

        int ack;
        try {
            ack = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new BadRequestException(e);
        }
        if (ack < 1 || ack > replicationFactor) {
            throw new BadRequestException();
        }
        return ack;
    }

    private void sendToReplica(int replicaPort, String key, Request request, ReplicationQuorum quorum) {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        URI uri = URI.create("http://localhost:" + replicaPort + EntityHandler.PATH + "?id=" + encodedKey);
        HttpRequest replicaRequest = HttpRequest.newBuilder(uri)
            .timeout(REQUEST_TIMEOUT)
            .header(REPLICA_HEADER, "1")
            .method(request.method(), HttpRequest.BodyPublishers.ofByteArray(request.body()))
            .build();

        client.sendAsync(replicaRequest, HttpResponse.BodyHandlers.ofByteArray())
            .whenComplete((replicaResponse, error) -> quorum.add(error == null
                ? Response.builder()
                    .setStatus(replicaResponse.statusCode())
                    .setBody(replicaResponse.body())
                    .build()
                : Response.builder()
                    .setStatus(HttpStatus.SERVICE_UNAVAILABLE.code())
                    .build()));
    }

    private Response handleLocal(Request request) {
        try {
            return switch (request.method()) {
                case "GET" -> localHandler.get(request);
                case "PUT" -> localHandler.put(request);
                case "DELETE" -> localHandler.delete(request);
                default -> throw new IllegalArgumentException();
            };
        } catch (NoSuchElementException e) {
            return Response.builder()
                .setStatus(HttpStatus.NOT_FOUND.code())
                .build();
        } catch (IOException e) {
            return Response.builder()
                .setStatus(HttpStatus.SERVICE_UNAVAILABLE.code())
                .build();
        }
    }
}
